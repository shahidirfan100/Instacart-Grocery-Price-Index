// Instacart Grocery Price Index - Production-ready Apify Actor
// Hybrid approach: HTTP + Apollo GraphQL (Priority 1) → Playwright stealth fallback (Priority 2)
import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { gotScraping } from 'got-scraping';

const PRIORITIZED_TODO = [
    {
        priority: 'P0',
        title: 'Identify product detail JSON source and required context',
        details: 'Capture network calls for product pages; pinpoint GraphQL/REST/_next/data endpoints; confirm required cookies/headers (store/zipcode/session) and verify price/availability in the response.',
    },
    {
        priority: 'P1',
        title: 'Implement HTTP JSON detail fetcher (primary path)',
        details: 'Add a gotScraping-based fetcher for detail data, reuse session cookies, parse price/unit price/brand/stock/store from JSON, and merge into listing items.',
    },
    {
        priority: 'P1',
        title: 'Add _next/data fallback for detail pages',
        details: 'Extract buildId from HTML (__NEXT_DATA__ or next-data) and request /_next/data/{buildId}/products/{id}.json (or store path), then parse product fields.',
    },
    {
        priority: 'P2',
        title: 'Harden HTML-only fallback',
        details: 'Parse JSON-LD Product/Offer and semantic selectors via Cheerio; handle missing fields with safe defaults and keep it as secondary to JSON endpoints.',
    },
    {
        priority: 'P2',
        title: 'Ensure location + store context for prices',
        details: 'Bootstrap location (zip/store) via internal location API or cookies, persist session per crawl, and validate retailer/store identifiers for detail requests.',
    },
    {
        priority: 'P3',
        title: 'Production hardening and stealth',
        details: 'Add request queue, 429/202 backoff, retry budgets, and keep Playwright as last-resort with aggressive resource blocking and low concurrency.',
    },
];

const logTodoList = () => {
    log.info('Prioritized to-do list for detail-page reliability and production hardening:');
    for (const item of PRIORITIZED_TODO) {
        log.info(`${item.priority} - ${item.title} ${item.details}`);
    }
};

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            startUrl = 'https://www.instacart.com/store/safeway/categories/316-food/317-fresh-produce',
            startUrls = [],
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 10,
            zipcode = '94105',
            extractDetails = false,
            printTodoList = false,
            detailConcurrency: DETAIL_CONCURRENCY_RAW = 4,
            detailPlaywrightFallback = false,
            proxyConfiguration,
        } = input;

        // Hardcoded internal settings
        const dedupe = true;  // Always remove duplicates
        const DELAY_MS_VALUE = 2000;  // Fixed delay between requests

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 10;
        const DETAIL_CONCURRENCY = Number.isFinite(+DETAIL_CONCURRENCY_RAW)
            ? Math.min(10, Math.max(1, +DETAIL_CONCURRENCY_RAW))
            : 4;

        log.info(`🚀 Starting Instacart scraper | Target: ${RESULTS_WANTED} products | Max pages: ${MAX_PAGES}`);

        if (printTodoList) logTodoList();

        // Stealth user agents
        const USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        ];

        const getRandomUA = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
        const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms + Math.random() * 500));
        const toAbs = (href, base) => { try { return new URL(href, base).href; } catch { return null; } };

        // Build initial URLs and warn about generic category URLs
        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) {
            initial.push(...startUrls.map(u => typeof u === 'string' ? { url: u } : u));
        }
        if (startUrl && !initial.some(u => u.url === startUrl)) initial.push({ url: startUrl });
        if (!initial.length) initial.push({ url: 'https://www.instacart.com/store/safeway/categories/316-food/317-fresh-produce' });

        // Warn if using generic category URLs (no store = no price data)
        for (const req of initial) {
            const url = req.url || '';
            if (url.includes('/categories/') && !url.includes('/store/')) {
                log.warning(`⚠️ URL "${url}" is a generic category URL without a store - prices will NOT be available. Use store-specific URLs like: /store/safeway/categories/...`);
            }
        }

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;
        let saved = 0;
        const seenProductIds = new Set();
        let usePlaywright = false;
        let nextBuildId = null;
        const sessionCookies = new Map();

        // ==================== PARSING FUNCTIONS ====================

        /**
         * Decode HTML entities in Apollo state JSON
         */
        function decodeHtmlEntities(str) {
            if (!str) return str;
            return str
                .replace(/&amp;/g, '&')
                .replace(/&lt;/g, '<')
                .replace(/&gt;/g, '>')
                .replace(/&quot;/g, '"')
                .replace(/&#39;/g, "'")
                .replace(/&#x27;/g, "'")
                .replace(/&#x2F;/g, '/')
                .replace(/&apos;/g, "'")
                .replace(/&#(\d+);/g, (_, dec) => String.fromCharCode(dec))
                .replace(/&#x([0-9A-Fa-f]+);/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
        }

        /**
         * Clean image URL by removing srcset artifacts and commas
         */
        function cleanImageUrl(url) {
            if (!url) return null;
            // Remove trailing commas, spaces, and srcset descriptors (like 2x, 197w)
            let cleaned = url.trim()
                .replace(/,$/, '')           // Remove trailing comma
                .replace(/\s+\d+[wx].*$/, '') // Remove srcset descriptors
                .replace(/,$/, '')           // Remove any remaining trailing comma
                .trim();
            return cleaned || null;
        }

        /**
         * PRIORITY 1: Extract from Apollo GraphQL state (node-apollo-state)
         */
        function extractApolloState($, options = {}) {
            const { quiet = false } = options;
            const info = quiet ? log.debug : log.info;
            const warn = quiet ? log.debug : log.warning;
            try {
                const apolloScript = $('script#node-apollo-state');
                if (!apolloScript.length) {
                    info('⚠️ Apollo state script not found in page');
                    return null;
                }

                let rawData = apolloScript.html() || apolloScript.text() || '';
                info(`📊 Apollo script found, raw length: ${rawData.length} chars`);

                if (!rawData || rawData.length < 100) {
                    warn('Apollo state is empty or too short');
                    return null;
                }

                // Step 1: Decode URL encoding first (handles %7B, %22, etc.)
                if (rawData.includes('%7B') || rawData.includes('%22')) {
                    try {
                        rawData = decodeURIComponent(rawData);
                        log.debug('URL decoding applied to Apollo state');
                    } catch (e) {
                        log.debug('URL decoding not needed or failed, continuing...');
                    }
                }

                // Step 2: Decode HTML entities
                const decodedData = decodeHtmlEntities(rawData);

                // Try to find JSON object boundaries
                let jsonString = decodedData.trim();
                if (!jsonString.startsWith('{')) {
                    const jsonMatch = jsonString.match(/\{[\s\S]*\}/);
                    if (jsonMatch) {
                        jsonString = jsonMatch[0];
                    }
                }

                if (jsonString && jsonString.startsWith('{')) {
                    const parsed = JSON.parse(jsonString);
                    const keyCount = Object.keys(parsed).length;
                    info(`✅ Apollo state parsed: ${keyCount} top-level keys`);

                    // Log sample keys for debugging
                    const sampleKeys = Object.keys(parsed).slice(0, 5);
                    log.debug(`Sample Apollo keys: ${sampleKeys.join(', ')}`);

                    return parsed;
                }

                warn('Apollo state does not contain valid JSON');
            } catch (e) {
                warn(`Apollo state parsing failed: ${e.message}`);
                log.debug(`Error details: ${e.stack?.slice(0, 200)}`);
            }
            return null;
        }

        /**
         * Extract products dynamically from Apollo cache
         * Handles Instacart's nested LandingTaxonomyProducts structure
         */
        function extractProductsFromApollo(apolloData, baseUrl) {
            const products = [];
            if (!apolloData || typeof apolloData !== 'object') return products;

            try {
                // Strategy 1: Look for LandingTaxonomyProducts keys (Instacart's structure)
                for (const [key, value] of Object.entries(apolloData)) {
                    if (!value || typeof value !== 'object') continue;

                    // Handle LandingTaxonomyProducts structure
                    if (key.startsWith('LandingTaxonomyProducts:') || key.includes('TaxonomyProducts')) {
                        log.debug(`Found taxonomy products key: ${key}`);

                        // Value is an object with query parameters as keys
                        for (const [queryKey, queryData] of Object.entries(value)) {
                            if (!queryData || typeof queryData !== 'object') continue;

                            // Look for landingTaxonomyProducts.products
                            const productsArray = queryData?.landingTaxonomyProducts?.products ||
                                queryData?.products ||
                                [];

                            if (Array.isArray(productsArray)) {
                                log.info(`Found ${productsArray.length} products in ${key}`);

                                for (const item of productsArray) {
                                    const product = extractLandingProduct(item, baseUrl);
                                    if (product.name || product.product_id) {
                                        products.push(product);
                                    }
                                }
                            }
                        }
                    }

                    // ========== NEW: Handle Items: structure for STORE-SPECIFIC pages ==========
                    // Store pages (e.g., /store/safeway/...) use Items: keys with items[] arrays
                    // These contain PRICE data at: item.price.viewSection.itemCard.priceString
                    if (key.startsWith('Items:')) {
                        log.debug(`Found store Items key: ${key}`);

                        // Items: value is an object with query parameters as keys
                        for (const [queryKey, queryData] of Object.entries(value)) {
                            if (!queryData || typeof queryData !== 'object') continue;

                            // Get items array
                            const itemsArray = queryData?.items || [];

                            if (Array.isArray(itemsArray) && itemsArray.length > 0) {
                                log.info(`Found ${itemsArray.length} products in ${key} (store page)`);

                                for (const item of itemsArray) {
                                    const product = extractLandingProduct(item, baseUrl);
                                    if (product.name || product.product_id) {
                                        products.push(product);
                                    }
                                }
                            }
                        }
                    }

                    // Also check for direct Product: keys (fallback for other Apollo structures)
                    if (key.startsWith('Product:') || key.startsWith('Item:') ||
                        value.__typename === 'Product' || value.__typename === 'Item') {
                        const product = extractProductFields(value, baseUrl);
                        if (product.name || product.product_id) {
                            products.push(product);
                        }
                    }
                }

                // Strategy 2: Check ROOT_QUERY for product arrays (fallback)
                const rootQuery = apolloData.ROOT_QUERY || apolloData.root_query || {};
                for (const [queryKey, queryValue] of Object.entries(rootQuery)) {
                    if (!queryValue) continue;
                    const productArrays = findProductArrays(queryValue, apolloData);
                    for (const arr of productArrays) {
                        for (const item of arr) {
                            const product = extractProductFields(item, baseUrl);
                            if (product.name || product.product_id) {
                                products.push(product);
                            }
                        }
                    }
                }

                log.info(`✅ Apollo extraction: Found ${products.length} products`);
            } catch (e) {
                log.warning(`Apollo product extraction error: ${e.message}`);
            }

            return products;
        }

        /**
         * Extract retailer/store name from URL path
         */
        function extractRetailerFromUrl(url) {
            if (!url) return 'Instacart';
            try {
                const urlObj = new URL(url);
                // Pattern: /store/{retailerSlug}/... or ?retailerSlug={retailer}
                const storeMatch = urlObj.pathname.match(/\/store\/([^\/]+)/);
                if (storeMatch && storeMatch[1]) {
                    // Capitalize retailer name (e.g., "safeway" -> "Safeway")
                    return storeMatch[1].charAt(0).toUpperCase() + storeMatch[1].slice(1).toLowerCase();
                }
                // Check URL params
                const retailerParam = urlObj.searchParams.get('retailerSlug') || urlObj.searchParams.get('retailer');
                if (retailerParam) {
                    return retailerParam.charAt(0).toUpperCase() + retailerParam.slice(1).toLowerCase();
                }
            } catch (e) {
                log.debug(`Failed to extract retailer from URL: ${e.message}`);
            }
            return 'Instacart';
        }

        /**
         * Extract retailer/store slug from URL path
         */
        function extractRetailerSlugFromUrl(url) {
            if (!url) return null;
            try {
                const urlObj = new URL(url);
                const storeMatch = urlObj.pathname.match(/\/store\/([^\/]+)/);
                if (storeMatch && storeMatch[1]) return storeMatch[1];
                const retailerParam = urlObj.searchParams.get('retailerSlug') || urlObj.searchParams.get('retailer');
                if (retailerParam) return retailerParam;
            } catch (e) {
                log.debug(`Failed to extract retailer slug from URL: ${e.message}`);
            }
            return null;
        }

        /**
         * Extract product from Instacart's LandingLandingProduct structure
         * Now extracts: price, original_price, brand, unit_price, and dynamic retailer/store
         */
        function extractLandingProduct(item, baseUrl) {
            if (!item || typeof item !== 'object') return {};

            const id = item.id || item.productId || null;
            const name = item.name || item.title || null;
            const size = item.size || null;
            const landingParam = item.landingParam || null;

            // ========== PRICE EXTRACTION ==========
            // CORRECT Apollo paths for store-specific pages:
            // - price.viewSection.itemCard.priceString (e.g., "$5.99")
            // - price.viewSection.itemCard.plainFullPriceString for original price (e.g., "$7.99")
            // - price.viewSection.itemDetails.pricePerUnitString for unit price (e.g., "$0.50/ct")
            let price = null;
            let originalPrice = null;
            let unitPrice = null;

            // Primary path: item.price.viewSection.itemCard (store-specific pages)
            const priceSection = item.price?.viewSection || {};
            const itemCard = priceSection.itemCard || {};
            const itemDetails = priceSection.itemDetails || {};

            // Current price from itemCard
            price = itemCard.priceString || itemCard.price ||
                priceSection.priceString || priceSection.price ||
                item.priceString || item.price ||
                null;

            // Original/Was Price from itemCard (plainFullPriceString or fullPriceString)
            originalPrice = itemCard.plainFullPriceString || itemCard.fullPriceString ||
                itemCard.originalPrice || itemCard.wasPrice ||
                priceSection.plainFullPriceString || priceSection.originalPrice ||
                item.originalPrice || item.wasPrice ||
                null;

            // Unit Price from itemDetails
            unitPrice = itemDetails.pricePerUnitString || itemDetails.unitPrice ||
                priceSection.pricePerUnitString || priceSection.unitPrice ||
                item.unitPrice || item.pricePerUnit ||
                null;

            // Fallback: check legacy viewSection.priceInfo structure
            const viewSection = item.viewSection || item.image?.viewSection || {};
            const priceInfo = viewSection.priceInfo || viewSection.pricing || item.priceInfo || {};

            if (!price) {
                price = priceInfo.price || priceInfo.currentPrice || priceInfo.priceString ||
                    viewSection.price || viewSection.currentPrice || null;
            }
            if (!originalPrice) {
                originalPrice = priceInfo.originalPrice || priceInfo.wasPrice ||
                    viewSection.originalPrice || null;
            }
            if (!unitPrice) {
                unitPrice = priceInfo.unitPrice || priceInfo.pricePerUnit ||
                    viewSection.unitPrice || null;
            }

            // ========== BRAND EXTRACTION ==========
            const brand = item.brand || item.brandName || item.brandInfo?.name ||
                viewSection.brand || viewSection.brandName ||
                item.manufacturer || null;

            // ========== IMAGE EXTRACTION ==========
            let imageUrl = null;
            const templateUrl = item.image?.viewSection?.productImage?.templateUrl ||
                item.image?.url || item.image?.templateUrl ||
                item.imageUrl || item.primaryImageUrl ||
                viewSection.productImage?.templateUrl ||
                null;

            if (templateUrl) {
                // Replace {width=}x{height=} placeholders with actual dimensions
                imageUrl = templateUrl
                    .replace('{width=}', '400')
                    .replace('{height=}', '400')
                    .replace('{width}', '400')
                    .replace('{height}', '400');
            }

            // Build product URL from landingParam
            const productUrl = landingParam
                ? `https://www.instacart.com/products/${landingParam}`
                : (id ? `https://www.instacart.com/products/${id}` : null);

            // ========== RETAILER/STORE EXTRACTION ==========
            // Try to get from item first, then fall back to URL parsing
            const retailer = item.retailerName || item.retailer || item.storeName || item.store ||
                viewSection.retailerName || viewSection.storeName ||
                extractRetailerFromUrl(baseUrl);
            const storeSlug = item.retailerSlug || item.storeSlug || extractRetailerSlugFromUrl(baseUrl);

            // ========== AVAILABILITY/STOCK STATUS ==========
            const inStock = item.inStock !== false &&
                item.available !== false &&
                item.isAvailable !== false &&
                item.availability?.status !== 'out_of_stock' &&
                viewSection.available !== false;

            return {
                product_id: id,
                name: name,
                brand: brand,
                price: typeof price === 'number' ? price : parsePrice(price),
                original_price: typeof originalPrice === 'number' ? originalPrice : parsePrice(originalPrice),
                unit_price: unitPrice,
                size: size,
                image_url: imageUrl ? cleanImageUrl(imageUrl) : null,
                product_url: productUrl,
                in_stock: inStock,
                store: retailer,
                store_slug: storeSlug,
                category: null, // Will be set from URL path
                timestamp: new Date().toISOString(),
                extraction_method: 'apollo_graphql'
            };
        }

        /**
         * Recursively find arrays that contain product-like objects
         */
        function findProductArrays(obj, apolloData, depth = 0) {
            const results = [];
            if (depth > 5 || !obj) return results;

            if (Array.isArray(obj)) {
                // Check if array contains products
                const hasProducts = obj.some(item =>
                    item && typeof item === 'object' &&
                    (item.__typename === 'Product' || item.__typename === 'Item' ||
                        item.name || item.productId || item.id)
                );
                if (hasProducts) {
                    // Resolve Apollo references if needed
                    const resolved = obj.map(item => {
                        if (item && item.__ref && apolloData[item.__ref]) {
                            return apolloData[item.__ref];
                        }
                        return item;
                    });
                    results.push(resolved);
                }
            } else if (typeof obj === 'object') {
                // Check for Apollo reference
                if (obj.__ref && apolloData[obj.__ref]) {
                    results.push(...findProductArrays(apolloData[obj.__ref], apolloData, depth + 1));
                }

                // Recurse into object properties
                for (const value of Object.values(obj)) {
                    results.push(...findProductArrays(value, apolloData, depth + 1));
                }
            }

            return results;
        }

        /**
         * Extract standard fields from a product object
         */
        function extractProductFields(product, baseUrl) {
            if (!product || typeof product !== 'object') return {};

            const id = product.id || product.productId || product.product_id ||
                product.legacyId || product.sku || null;

            const name = product.name || product.title || product.displayName || null;

            const price = product.price || product.currentPrice ||
                product.pricing?.price || product.priceString || null;

            const originalPrice = product.originalPrice || product.wasPrice ||
                product.pricing?.originalPrice || null;

            const imageUrl = product.image?.url || product.imageUrl ||
                product.primaryImage?.url || product.thumbnail || null;

            const productUrl = product.url || product.permalink || product.link ||
                (product.legacyId ? `https://www.instacart.com/store/items/item_${product.legacyId}` : null) ||
                (id ? `https://www.instacart.com/products/${id}` : null);

            const storeName = extractRetailerFromUrl(productUrl || baseUrl);
            const storeSlug = extractRetailerSlugFromUrl(productUrl || baseUrl);
            const size = product.size || product.packageSize || product.unitSize || null;
            const brand = product.brand || product.brandName || null;
            const inStock = product.inStock !== false &&
                product.availability?.status !== 'out_of_stock' &&
                product.isAvailable !== false;

            return {
                product_id: id,
                name: name,
                brand: brand,
                price: typeof price === 'number' ? price : parsePrice(price),
                original_price: typeof originalPrice === 'number' ? originalPrice : parsePrice(originalPrice),
                size: size,
                image_url: imageUrl ? cleanImageUrl(toAbs(imageUrl, baseUrl)) : null,
                product_url: productUrl ? toAbs(productUrl, baseUrl) : null,
                in_stock: inStock,
                store: storeName,
                store_slug: storeSlug,
                timestamp: new Date().toISOString(),
                extraction_method: 'apollo_graphql'
            };
        }

        /**
         * Parse price string to number
         */
        function parsePrice(priceStr) {
            if (!priceStr) return null;
            if (typeof priceStr === 'number') return priceStr;
            const cleaned = String(priceStr).replace(/[^0-9.]/g, '');
            const parsed = parseFloat(cleaned);
            return isNaN(parsed) ? null : parsed;
        }

        /**
         * Safe JSON parse helper
         */
        function safeJsonParse(value) {
            if (!value) return null;
            if (typeof value === 'object') return value;
            try {
                return JSON.parse(value);
            } catch {
                return null;
            }
        }

        function normalizeHtmlBody(body) {
            if (body === null || body === undefined) return null;
            if (Buffer.isBuffer(body)) return body.toString('utf-8');
            if (typeof body === 'string') return body;
            if (typeof body === 'object') {
                try {
                    return JSON.stringify(body);
                } catch {
                    return null;
                }
            }
            return String(body);
        }

        /**
         * Extract Next.js data object from HTML
         */
        function extractNextData($) {
            const nextScript = $('script#__NEXT_DATA__').first();
            if (!nextScript.length) return null;
            const raw = nextScript.html() || nextScript.text() || '';
            return safeJsonParse(raw);
        }

        /**
         * Extract product ID from URL
         */
        function extractProductIdFromUrl(url) {
            if (!url) return null;
            try {
                const urlObj = new URL(url);
                const match = urlObj.pathname.match(/\/products\/([^\/?#]+)/);
                if (match && match[1]) return match[1];
                const storeMatch = urlObj.pathname.match(/\/store\/[^\/]+\/products\/([^\/?#]+)/);
                if (storeMatch && storeMatch[1]) return storeMatch[1];
            } catch (e) {
                log.debug(`Failed to extract product id from URL: ${e.message}`);
            }
            return null;
        }

        /**
         * PRIORITY 2: HTML parsing fallback
         */
        function extractFromHTML($, baseUrl) {
            const products = [];
            const updatedProducts = [];
            try {
                // Multiple selector strategies for Instacart
                const selectors = [
                    'a[href*="/products/"]',
                    'a[href*="/store/items/"]',
                    '[data-testid*="product"]',
                    '[data-testid*="item-card"]',
                    '[class*="ItemCard"]',
                ];

                const seen = new Set();

                for (const selector of selectors) {
                    $(selector).each((_, el) => {
                        const $el = $(el);
                        const href = $el.attr('href') || $el.find('a').first().attr('href');
                        const fullUrl = href ? toAbs(href, baseUrl) : null;

                        if (fullUrl && seen.has(fullUrl)) return;
                        if (fullUrl) seen.add(fullUrl);

                        // Find product name
                        const name = $el.find('[class*="ItemName"], [class*="product-name"], h3, h4, [data-testid*="name"]')
                            .first().text().trim() ||
                            $el.attr('aria-label') ||
                            $el.text().trim().split('\n')[0]?.trim();

                        // Find price
                        const priceText = $el.find('[class*="Price"], [data-testid*="price"]').first().text().trim();
                        const price = parsePrice(priceText);

                        // Find image - handle srcset format properly
                        let imgSrc = $el.find('img').attr('src');
                        if (!imgSrc) {
                            const srcset = $el.find('img').attr('srcset');
                            if (srcset) {
                                // srcset format: "url1 197w, url2 394w" - get first URL
                                imgSrc = srcset.split(',')[0]?.split(' ')[0];
                            }
                        }
                        imgSrc = cleanImageUrl(imgSrc);

                        if (name && name.length > 2 && name.length < 200) {
                            products.push({
                                name: name,
                                price: price,
                                image_url: imgSrc ? cleanImageUrl(toAbs(imgSrc, baseUrl)) : null,
                                product_url: fullUrl,
                                store: 'Instacart',
                                in_stock: true,
                                timestamp: new Date().toISOString(),
                                extraction_method: 'html_parsing'
                            });
                        }
                    });
                }

                // Dedupe by name
                const uniqueProducts = [];
                const seenNames = new Set();
                for (const p of products) {
                    const key = p.name?.toLowerCase();
                    if (key && !seenNames.has(key)) {
                        seenNames.add(key);
                        uniqueProducts.push(p);
                    }
                }

                log.info(`📄 HTML extraction: Found ${uniqueProducts.length} unique products`);
                return uniqueProducts;
            } catch (e) {
                log.debug(`HTML extraction failed: ${e.message}`);
            }
            return updatedProducts;
        }

        // ==================== DETAIL HELPERS ====================

        function buildProductUrl(product, baseUrl) {
            if (!product) return null;
            const productId = product.product_id || extractProductIdFromUrl(product.product_url);
            const storeSlug = product.store_slug ||
                extractRetailerSlugFromUrl(product.product_url) ||
                extractRetailerSlugFromUrl(baseUrl);
            if (storeSlug && productId) {
                return `https://www.instacart.com/store/${storeSlug}/products/${productId}`;
            }
            if (productId) {
                return `https://www.instacart.com/products/${productId}`;
            }
            if (product.product_url) return toAbs(product.product_url, baseUrl);
            return null;
        }

        function buildNextDataUrl(productUrl, buildId) {
            if (!productUrl || !buildId) return null;
            try {
                const urlObj = new URL(productUrl);
                const path = urlObj.pathname;
                const query = urlObj.search || '';
                return `https://www.instacart.com/_next/data/${buildId}${path}.json${query}`;
            } catch {
                return null;
            }
        }

        function findProductInObject(obj, productId, depth = 0) {
            if (!obj || depth > 6) return null;
            if (Array.isArray(obj)) {
                for (const item of obj) {
                    const found = findProductInObject(item, productId, depth + 1);
                    if (found) return found;
                }
                return null;
            }
            if (typeof obj !== 'object') return null;

            const id = obj.id || obj.productId || obj.product_id || obj.legacyId || obj.sku;
            if (productId && id && String(id) === String(productId)) return obj;
            if (!productId && (obj.__typename === 'Product' || obj.__typename === 'Item') && (obj.name || obj.title)) {
                return obj;
            }

            for (const value of Object.values(obj)) {
                const found = findProductInObject(value, productId, depth + 1);
                if (found) return found;
            }
            return null;
        }

        function extractDetailFromProductObject(productObj, baseUrl) {
            if (!productObj || typeof productObj !== 'object') return null;
            const hasViewSection = productObj.price?.viewSection || productObj.viewSection;
            const detail = hasViewSection
                ? extractLandingProduct(productObj, baseUrl)
                : extractProductFields(productObj, baseUrl);

            const unitPrice = productObj.unitPrice ||
                productObj.pricePerUnit ||
                productObj.pricing?.unitPrice ||
                productObj.price?.unitPrice ||
                productObj.price?.viewSection?.itemDetails?.pricePerUnitString;
            if (unitPrice && !detail.unit_price) detail.unit_price = unitPrice;

            const brand = productObj.brandName || productObj.brand || productObj.brandInfo?.name;
            if (brand && !detail.brand) detail.brand = brand;

            detail.extraction_method = detail.extraction_method || 'detail_json';
            return detail;
        }

        function extractDetailFromApolloState(apolloData, product, baseUrl) {
            if (!apolloData || typeof apolloData !== 'object') return null;
            const productId = product?.product_id || extractProductIdFromUrl(product?.product_url);

            if (productId) {
                const directKeys = [`Item:${productId}`, `Product:${productId}`];
                for (const key of directKeys) {
                    if (apolloData[key]) {
                        return extractDetailFromProductObject(apolloData[key], baseUrl);
                    }
                }
            }

            for (const value of Object.values(apolloData)) {
                if (!value || typeof value !== 'object') continue;
                const id = value.id || value.productId || value.product_id || value.legacyId;
                if (productId && id && String(id) === String(productId)) {
                    return extractDetailFromProductObject(value, baseUrl);
                }
            }

            const candidates = extractProductsFromApollo(apolloData, baseUrl);
            if (productId) {
                return candidates.find(p => String(p.product_id) === String(productId)) || null;
            }
            if (product?.product_url) {
                return candidates.find(p => p.product_url === product.product_url) || null;
            }
            return null;
        }

        function extractDetailFromNextData(nextData, product, baseUrl) {
            if (!nextData) return null;
            const pageProps = nextData?.props?.pageProps || nextData?.pageProps || nextData?.props || null;
            const apolloState = pageProps?.apolloState ||
                pageProps?.initialApolloState ||
                pageProps?.apolloCache ||
                pageProps?.apollo;
            if (apolloState) {
                const detail = extractDetailFromApolloState(apolloState, product, baseUrl);
                if (detail) {
                    detail.extraction_method = 'next_data_apollo';
                    return detail;
                }
            }

            const productId = product?.product_id || extractProductIdFromUrl(product?.product_url);
            const productObj = findProductInObject(nextData, productId);
            if (productObj) {
                const detail = extractDetailFromProductObject(productObj, baseUrl);
                detail.extraction_method = 'next_data_json';
                return detail;
            }
            return null;
        }

        function findJsonLdProduct(data) {
            if (!data) return null;
            if (Array.isArray(data)) {
                for (const item of data) {
                    const found = findJsonLdProduct(item);
                    if (found) return found;
                }
                return null;
            }
            if (data['@graph'] && Array.isArray(data['@graph'])) {
                return findJsonLdProduct(data['@graph']);
            }
            if (data['@type'] === 'Product') return data;
            return null;
        }

        function extractDetailFromHtml($, baseUrl) {
            const detail = {};

            const ldScripts = $('script[type="application/ld+json"]');
            for (const el of ldScripts.toArray()) {
                const raw = $(el).text();
                const parsed = safeJsonParse(raw);
                const product = findJsonLdProduct(parsed);
                if (product) {
                    const offers = Array.isArray(product.offers) ? product.offers[0] : product.offers;
                    if (offers?.price) detail.price = parsePrice(offers.price);
                    if (offers?.priceCurrency && !detail.currency) detail.currency = offers.priceCurrency;
                    const brand = typeof product.brand === 'string' ? product.brand : product.brand?.name;
                    if (brand) detail.brand = brand;
                    const image = Array.isArray(product.image) ? product.image[0] : product.image;
                    if (image) detail.image_url = cleanImageUrl(toAbs(image, baseUrl));
                    if (product.name) detail.name = product.name;
                    if (product.sku) detail.product_id = product.sku;
                    break;
                }
            }

            const priceMeta = $('meta[property="product:price:amount"]').attr('content') ||
                $('meta[itemprop="price"]').attr('content');
            if (!detail.price && priceMeta) detail.price = parsePrice(priceMeta);

            const brandMeta = $('meta[property="product:brand"]').attr('content') ||
                $('meta[itemprop="brand"]').attr('content');
            if (!detail.brand && brandMeta) detail.brand = brandMeta;

            const ogImage = $('meta[property="og:image"]').attr('content');
            if (!detail.image_url && ogImage) detail.image_url = cleanImageUrl(toAbs(ogImage, baseUrl));

            const unitPriceText = $('[data-testid*="pricePerUnit"], [class*="unit-price"]').first().text().trim();
            if (!detail.unit_price && unitPriceText) detail.unit_price = unitPriceText;

            if (Object.keys(detail).length === 0) return null;
            detail.extraction_method = detail.extraction_method || 'html_fallback';
            return detail;
        }

        function mergeProductDetails(target, detail, method) {
            if (!target || !detail) return false;
            let updated = false;
            const fields = [
                'product_id',
                'price',
                'original_price',
                'unit_price',
                'brand',
                'size',
                'image_url',
                'product_url',
                'in_stock',
                'store',
                'store_slug',
                'currency',
            ];

            for (const field of fields) {
                const value = detail[field];
                if (value === undefined || value === null || value === '') continue;
                const current = target[field];
                const shouldReplace = current === undefined || current === null || current === '' ||
                    (field === 'store' && current === 'Instacart') ||
                    (field === 'price' && (!current || current === 0));
                if (shouldReplace) {
                    target[field] = value;
                    updated = true;
                }
            }

            if (updated && method) {
                target.detail_extraction_method = method;
                target.enriched_at = new Date().toISOString();
            }
            return updated;
        }

        // ==================== HTTP HELPERS ====================

        function getCookieHeader() {
            if (!sessionCookies.size) return null;
            return Array.from(sessionCookies.entries())
                .map(([name, value]) => `${name}=${value}`)
                .join('; ');
        }

        function updateCookiesFromResponse(response) {
            const setCookie = response?.headers?.['set-cookie'];
            if (!setCookie) return;
            const cookies = Array.isArray(setCookie) ? setCookie : [setCookie];
            for (const cookie of cookies) {
                const pair = cookie.split(';')[0];
                const eqIndex = pair.indexOf('=');
                if (eqIndex <= 0) continue;
                const name = pair.slice(0, eqIndex).trim();
                const value = pair.slice(eqIndex + 1).trim();
                if (name) sessionCookies.set(name, value);
            }
        }

        function buildHeaders(options = {}) {
            const {
                accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                referer = 'https://www.instacart.com/',
                origin = 'https://www.instacart.com',
                fetchMode = 'navigate',
                fetchDest = 'document',
                fetchSite = 'none',
                fetchUser = '?1',
            } = options;

            const headers = {
                'User-Agent': getRandomUA(),
                'Accept': accept,
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Sec-Fetch-Dest': fetchDest,
                'Sec-Fetch-Mode': fetchMode,
                'Sec-Fetch-Site': fetchSite,
                'Sec-Fetch-User': fetchUser,
                'Upgrade-Insecure-Requests': '1',
                'Referer': referer,
                'Origin': origin,
            };

            if (fetchUser === null || fetchUser === undefined) {
                delete headers['Sec-Fetch-User'];
            }

            const cookieHeader = getCookieHeader();
            if (cookieHeader) headers['Cookie'] = cookieHeader;
            return headers;
        }

        // ==================== HTTP REQUEST METHOD (PRIORITY 1) ====================

        async function fetchWithHTTP(url, retryCount = 0, options = {}) {
            const MAX_RETRIES = 3;

            try {
                const response = await gotScraping({
                    url,
                    headers: buildHeaders(options),
                    timeout: { request: 30000 },
                    retry: { limit: 0 },
                    throwHttpErrors: false,
                    responseType: 'text',
                    proxyUrl: proxyConf ? await proxyConf.newUrl() : undefined,
                });

                updateCookiesFromResponse(response);
                const bodyText = normalizeHtmlBody(response.body);

                if (response.statusCode === 200) {
                    return bodyText;
                }

                // Handle 202 (Accepted/Processing) - common for Instacart async pages
                if (response.statusCode === 202) {
                    // Check if 202 response still contains useful Apollo data
                    if (bodyText && bodyText.includes('node-apollo-state')) {
                        log.debug(`202 response contains Apollo data, using it`);
                        return bodyText;
                    }

                    // Retry with exponential backoff
                    if (retryCount < MAX_RETRIES) {
                        const delay = 1000 * Math.pow(2, retryCount); // 1s, 2s, 4s
                        log.debug(`202 status, retrying in ${delay}ms (attempt ${retryCount + 1}/${MAX_RETRIES})`);
                        await sleep(delay);
                        return fetchWithHTTP(url, retryCount + 1, options);
                    }

                    log.debug(`202 after ${MAX_RETRIES} retries, returning response body anyway`);
                    return bodyText; // Return anyway, might have partial data
                }

                if ([429, 403, 503].includes(response.statusCode) && retryCount < MAX_RETRIES) {
                    const delay = 1000 * Math.pow(2, retryCount); // 1s, 2s, 4s
                    log.debug(`HTTP ${response.statusCode}, retrying in ${delay}ms (attempt ${retryCount + 1}/${MAX_RETRIES})`);
                    await sleep(delay);
                    return fetchWithHTTP(url, retryCount + 1, options);
                }

                log.warning(`HTTP request returned status ${response.statusCode}`);
                return null;
            } catch (e) {
                log.warning(`HTTP request failed: ${e.message}`);
                return null;
            }
        }

        async function fetchJsonWithHTTP(url, retryCount = 0, options = {}) {
            const MAX_RETRIES = 3;
            const requestOptions = {
                accept: 'application/json, text/plain, */*',
                fetchMode: 'cors',
                fetchDest: 'empty',
                fetchSite: 'same-origin',
                fetchUser: null,
                ...options,
            };

            try {
                const response = await gotScraping({
                    url,
                    headers: buildHeaders(requestOptions),
                    timeout: { request: 30000 },
                    retry: { limit: 0 },
                    throwHttpErrors: false,
                    responseType: 'text',
                    proxyUrl: proxyConf ? await proxyConf.newUrl() : undefined,
                });

                updateCookiesFromResponse(response);

                const bodyText = normalizeHtmlBody(response.body);
                if (response.statusCode === 200 && bodyText) {
                    const parsed = safeJsonParse(bodyText);
                    if (parsed) return parsed;
                }

                if ([202, 429, 403, 503].includes(response.statusCode) && retryCount < MAX_RETRIES) {
                    const delay = 1000 * Math.pow(2, retryCount);
                    log.debug(`JSON HTTP ${response.statusCode}, retrying in ${delay}ms (attempt ${retryCount + 1}/${MAX_RETRIES})`);
                    await sleep(delay);
                    return fetchJsonWithHTTP(url, retryCount + 1, options);
                }

                log.debug(`JSON request returned status ${response.statusCode}`);
                return null;
            } catch (e) {
                log.debug(`JSON request failed: ${e.message}`);
                return null;
            }
        }

        // ==================== PLAYWRIGHT FALLBACK (PRIORITY 2) ====================

        async function fetchWithPlaywright(url) {
            log.info(`🎭 Using Playwright stealth mode for: ${url}`);

            // Use closure variable to capture HTML from the request handler
            let extractedHtml = null;

            const playwrightCrawler = new PlaywrightCrawler({
                proxyConfiguration: proxyConf,
                maxRequestRetries: 2,
                requestHandlerTimeoutSecs: 60,
                navigationTimeoutSecs: 30,
                headless: true,
                maxConcurrency: 1,

                launchContext: {
                    launchOptions: {
                        args: [
                            '--disable-blink-features=AutomationControlled',
                            '--disable-dev-shm-usage',
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                        ],
                    },
                },

                preNavigationHooks: [
                    async ({ page }) => {
                        // Block heavy resources for speed
                        await page.route('**/*', (route) => {
                            const resourceType = route.request().resourceType();
                            if (['image', 'media', 'font', 'stylesheet'].includes(resourceType)) {
                                return route.abort();
                            }
                            return route.continue();
                        });

                        // Stealth: Override navigator properties
                        await page.addInitScript(() => {
                            Object.defineProperty(navigator, 'webdriver', { get: () => false });
                            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                            window.chrome = { runtime: {} };
                        });

                        await page.setExtraHTTPHeaders({
                            'Accept-Language': 'en-US,en;q=0.9',
                        });
                    },
                ],

                async requestHandler({ page, request }) {
                    // Wait for content to load
                    await page.waitForLoadState('domcontentloaded');
                    await page.waitForTimeout(1500);

                    // Get page HTML and store in closure variable
                    extractedHtml = await page.content();
                    log.debug(`Playwright extracted ${extractedHtml?.length || 0} chars of HTML`);
                },

                failedRequestHandler({ request, error }) {
                    log.debug(`Playwright failed for ${request.url}: ${error.message}`);
                },
            });

            try {
                await playwrightCrawler.run([{ url }]);
            } catch (e) {
                log.warning(`Playwright crawl error: ${e.message}`);
            }

            return extractedHtml;
        }

        // ==================== MAIN SCRAPING LOGIC ====================

        async function scrapeUrl(url, pageNo = 1) {
            const products = [];
            try {
                // Add stealth delay
                if (pageNo > 1) {
                    await sleep(DELAY_MS_VALUE);
                }

                log.info(`?? Processing page ${pageNo}: ${url}`);

                // Try HTTP first (Priority 1)
                let html = await fetchWithHTTP(url);

                // If HTTP fails, use Playwright (Priority 2)
                if (!html && !usePlaywright) {
                    log.info('?? HTTP failed, switching to Playwright mode');
                    usePlaywright = true;
                }

                if (!html && usePlaywright) {
                    html = await fetchWithPlaywright(url);
                }

                if (!html) {
                    log.error(`? Failed to fetch: ${url}`);
                    return products;
                }

                // Parse HTML with Cheerio
                const $ = cheerioLoad(html);

                const nextData = extractNextData($);
                if (nextData?.buildId) {
                    if (!nextBuildId || nextBuildId !== nextData.buildId) {
                        nextBuildId = nextData.buildId;
                        log.debug(`Captured Next.js buildId: ${nextBuildId}`);
                    }
                }

                // Try Apollo extraction first (Priority 1)
                const apolloData = extractApolloState($);
                if (apolloData) {
                    const apolloProducts = extractProductsFromApollo(apolloData, url);
                    products.push(...apolloProducts);
                }

                // Fallback to HTML parsing if Apollo didn't yield results
                if (products.length === 0) {
                    const htmlProducts = extractFromHTML($, url);
                    products.push(...htmlProducts);
                }

                for (const product of products) {
                    if (!product.source_url) product.source_url = url;
                    if (!product.store_slug) {
                        product.store_slug = extractRetailerSlugFromUrl(product.product_url || url);
                    }
                }
            } catch (e) {
                log.warning(`Scrape failed for ${url}: ${e.message}`);
                log.debug(`Scrape error stack: ${e.stack?.slice(0, 500)}`);
                return products;
            }

            return products;
        }

        // ==================== DETAIL FETCH (HTTP FIRST) ====================

        async function fetchDetailForProductHTTP(product, baseUrl) {
            const productUrl = buildProductUrl(product, baseUrl || product?.source_url);
            if (!productUrl) return { updated: false };
            if (!product.product_url) product.product_url = productUrl;
            if (!product.store_slug) {
                product.store_slug = extractRetailerSlugFromUrl(productUrl);
            }
            if (!product.store || product.store === 'Instacart') {
                product.store = extractRetailerFromUrl(productUrl);
            }

            const referer = product.source_url || baseUrl || productUrl;
            let detail = null;

            if (nextBuildId) {
                const nextUrl = buildNextDataUrl(productUrl, nextBuildId);
                if (nextUrl) {
                    const nextData = await fetchJsonWithHTTP(nextUrl, 0, { referer: productUrl });
                    detail = extractDetailFromNextData(nextData, product, productUrl);
                    if (detail && mergeProductDetails(product, detail, detail.extraction_method)) {
                        return { updated: true, method: detail.extraction_method };
                    }
                }
            }

            const html = await fetchWithHTTP(productUrl, 0, { referer });
            if (!html) return { updated: false };

            const $ = cheerioLoad(html);
            const nextDataFromHtml = extractNextData($);
            if (nextDataFromHtml?.buildId && (!nextBuildId || nextBuildId !== nextDataFromHtml.buildId)) {
                nextBuildId = nextDataFromHtml.buildId;
                log.debug(`Updated Next.js buildId from detail page: ${nextBuildId}`);
            }

            if (nextDataFromHtml?.buildId) {
                const nextUrl = buildNextDataUrl(productUrl, nextDataFromHtml.buildId);
                if (nextUrl) {
                    const nextData = await fetchJsonWithHTTP(nextUrl, 0, { referer: productUrl });
                    detail = extractDetailFromNextData(nextData, product, productUrl);
                    if (detail && mergeProductDetails(product, detail, detail.extraction_method)) {
                        return { updated: true, method: detail.extraction_method };
                    }
                }
            }

            const apolloData = extractApolloState($, { quiet: true });
            detail = extractDetailFromApolloState(apolloData, product, productUrl);
            if (detail && mergeProductDetails(product, detail, detail.extraction_method || 'apollo_html')) {
                return { updated: true, method: detail.extraction_method || 'apollo_html' };
            }

            detail = extractDetailFromHtml($, productUrl);
            if (detail && mergeProductDetails(product, detail, detail.extraction_method || 'html_fallback')) {
                return { updated: true, method: detail.extraction_method || 'html_fallback' };
            }

            return { updated: false };
        }

        async function fetchProductDetailsHTTP(products, baseUrl) {
            if (!products.length) return [];
            log.info(`?? Fetching ${products.length} detail pages via HTTP/JSON...`);

            const updatedProducts = [];
            for (let i = 0; i < products.length; i += DETAIL_CONCURRENCY) {
                const batch = products.slice(i, i + DETAIL_CONCURRENCY);
                const results = await Promise.all(batch.map(p => fetchDetailForProductHTTP(p, baseUrl)));

                results.forEach((result, idx) => {
                    if (result?.updated) updatedProducts.push(batch[idx]);
                });

                if (i + DETAIL_CONCURRENCY < products.length) {
                    await sleep(400);
                }
            }

            log.info(`?? HTTP detail enrichment updated ${updatedProducts.length}/${products.length} products`);
            return updatedProducts;
        }

        // ==================== FETCH PRODUCT DETAIL PAGES WITH PLAYWRIGHT ====================

        /**
         * Fetch multiple product detail pages in parallel using Playwright
         * Uses shared Map to store extracted data for reliable data passing
         */
        async function fetchProductDetailsBatch(products) {
            if (!products.length) return [];
            usePlaywright = true;

            log.info(`🎭 Using Playwright to fetch ${products.length} detail pages...`);

            // Shared data store - Map keyed by URL
            const extractedDataMap = new Map();

            const playwrightCrawler = new PlaywrightCrawler({
                proxyConfiguration: proxyConf,
                maxRequestRetries: 1, // Fewer retries for speed
                requestHandlerTimeoutSecs: 30, // Faster timeout
                navigationTimeoutSecs: 20, // Faster navigation timeout
                maxConcurrency: Math.min(DETAIL_CONCURRENCY, 4), // Keep browser concurrency low
                headless: true,
                useSessionPool: true,
                persistCookiesPerSession: true,

                browserPoolOptions: {
                    maxOpenPagesPerBrowser: 5, // More pages per browser
                    retireBrowserAfterPageCount: 20, // Retire less frequently
                },

                launchContext: {
                    launchOptions: {
                        args: [
                            '--disable-blink-features=AutomationControlled',
                            '--disable-dev-shm-usage',
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                            '--disable-infobars',
                            '--disable-background-networking',
                            '--disable-default-apps',
                            '--disable-extensions',
                            '--disable-sync',
                            '--disable-translate',
                            '--metrics-recording-only',
                            '--mute-audio',
                            '--no-first-run',
                            '--ignore-certificate-errors',
                            '--disable-gpu', // Faster rendering
                            '--disable-software-rasterizer',
                        ],
                    },
                },

                preNavigationHooks: [
                    async ({ page, request }) => {
                        // Block ALL non-essential resources for maximum speed
                        await page.route('**/*', (route) => {
                            const resourceType = route.request().resourceType();
                            const url = route.request().url();
                            // Only allow document and XHR/fetch for data
                            if (['image', 'media', 'font', 'stylesheet'].includes(resourceType)) {
                                return route.abort();
                            }
                            // Block tracking/analytics scripts
                            if (url.includes('analytics') || url.includes('tracking') ||
                                url.includes('segment') || url.includes('gtag') ||
                                url.includes('facebook') || url.includes('google-analytics')) {
                                return route.abort();
                            }
                            return route.continue();
                        });

                        // Stealth: Override navigator properties
                        await page.addInitScript(() => {
                            Object.defineProperty(navigator, 'webdriver', { get: () => false });
                            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                            window.chrome = { runtime: {} };
                            delete navigator.__proto__.webdriver;
                        });

                        await page.setExtraHTTPHeaders({
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        });
                    },
                ],

                async requestHandler({ page, request }) {
                    const productUrl = request.url;
                    const productIndex = request.userData.productIndex;

                    try {
                        // Wait for DOM only (faster than load/networkidle)
                        await page.waitForLoadState('domcontentloaded');
                        await page.waitForTimeout(300); // Minimal wait for Apollo to populate

                        // Extract data using JavaScript in browser context
                        const extractedData = await page.evaluate(() => {
                            const result = { price: null, originalPrice: null, brand: null, unitPrice: null, store: null };

                            // Try Apollo state FIRST (most reliable for Instacart)
                            const apolloEl = document.getElementById('node-apollo-state');
                            if (apolloEl) {
                                try {
                                    const apolloData = JSON.parse(decodeURIComponent(apolloEl.textContent));
                                    for (const [key, value] of Object.entries(apolloData)) {
                                        if (key.startsWith('Items:') && typeof value === 'object') {
                                            for (const queryData of Object.values(value)) {
                                                if (queryData?.items?.[0]) {
                                                    const item = queryData.items[0];
                                                    const priceSection = item.price?.viewSection || {};
                                                    const itemCard = priceSection.itemCard || {};
                                                    const itemDetails = priceSection.itemDetails || {};

                                                    // Price from itemCard
                                                    if (!result.price && itemCard.priceString) {
                                                        result.price = parseFloat(itemCard.priceString.replace(/[^0-9.]/g, ''));
                                                    }
                                                    // Original price (was price)
                                                    if (!result.originalPrice && itemCard.plainFullPriceString) {
                                                        result.originalPrice = parseFloat(itemCard.plainFullPriceString.replace(/[^0-9.]/g, ''));
                                                    }
                                                    // Unit price from itemDetails
                                                    if (!result.unitPrice && itemDetails.pricePerUnitString) {
                                                        result.unitPrice = itemDetails.pricePerUnitString;
                                                    }
                                                    // Brand
                                                    if (!result.brand && item.brandName) {
                                                        result.brand = item.brandName;
                                                    }
                                                }
                                            }
                                        }
                                        // Store name
                                        if (key.startsWith('RetailersRetailer:') && typeof value === 'object') {
                                            if (!result.store && value.name) {
                                                result.store = value.name;
                                            }
                                        }
                                    }
                                } catch (e) { }
                            }

                            // Fallback: Try JSON-LD
                            if (!result.price) {
                                const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
                                for (const script of ldScripts) {
                                    try {
                                        const data = JSON.parse(script.textContent || '{}');
                                        const product = data['@type'] === 'Product' ? data :
                                            (data['@graph']?.find(i => i['@type'] === 'Product') || null);
                                        if (product) {
                                            if (product.offers?.price) result.price = parseFloat(product.offers.price);
                                            if (product.brand) result.brand = typeof product.brand === 'object' ? product.brand.name : product.brand;
                                        }
                                    } catch (e) { }
                                }
                            }

                            // Fallback: Extract from visible text
                            if (!result.price) {
                                const bodyText = document.body.innerText;
                                const priceMatch = bodyText.match(/\$(\d+\.?\d*)\s*each/i) || bodyText.match(/\$(\d+\.?\d*)/);
                                if (priceMatch) result.price = parseFloat(priceMatch[1]);
                            }

                            if (!result.unitPrice) {
                                const bodyText = document.body.innerText;
                                const unitMatch = bodyText.match(/\$(\d+\.?\d*)\s*\/\s*(\w+)/);
                                if (unitMatch) result.unitPrice = unitMatch[0];
                            }

                            return result;
                        });

                        // Store in shared Map
                        extractedDataMap.set(productUrl, { index: productIndex, data: extractedData });

                        if (extractedData.price) {
                            const origStr = extractedData.originalPrice ? ` (was $${extractedData.originalPrice})` : '';
                            log.info(`✅ [${productIndex + 1}] $${extractedData.price}${origStr} | ${extractedData.brand || 'Unknown'} | ${extractedData.unitPrice || ''}`);
                        }

                    } catch (e) {
                        log.debug(`Detail extraction failed for index ${productIndex}: ${e.message}`);
                    }
                },

                failedRequestHandler({ request, error }) {
                    log.debug(`Failed to fetch: ${request.url} - ${error.message}`);
                },
            });

            // Build requests for all products
            const requests = products.map((product, index) => {
                const productUrl = buildProductUrl(product, product.source_url);
                if (productUrl && !product.product_url) product.product_url = productUrl;
                return {
                    url: productUrl,
                    userData: { productIndex: index },
                    uniqueKey: productUrl,
                };
            }).filter(r => r.url);

            try {
                await playwrightCrawler.run(requests);

                // Merge extracted data back into products from shared Map
                for (const [, { index, data }] of extractedDataMap) {
                    if (products[index]) {
                        const detail = {
                            price: data.price,
                            original_price: data.originalPrice,
                            brand: data.brand,
                            unit_price: data.unitPrice,
                            store: data.store,
                        };
                        const updated = mergeProductDetails(products[index], detail, 'playwright_detail');
                        if (updated) updatedProducts.push(products[index]);
                    }
                }

                log.info(`📊 Extracted data from ${extractedDataMap.size}/${products.length} detail pages`);
            } catch (e) {
                log.warning(`Batch detail fetching error: ${e.message}`);
            }

            return products;
        }

        // ==================== RUN SCRAPER ====================

        const allProducts = [];
        let currentPage = 1;

        for (const startReq of initial) {
            const url = typeof startReq === 'string' ? startReq : startReq.url;

            while (currentPage <= MAX_PAGES && saved < RESULTS_WANTED) {
                const pageUrl = currentPage === 1 ? url : `${url}?page=${currentPage}`;
                const products = await scrapeUrl(pageUrl, currentPage);

                if (products.length === 0) {
                    log.info(`No products found on page ${currentPage}, stopping pagination`);
                    break;
                }

                // Process and push products immediately
                const productsToSave = [];
                for (const product of products) {
                    if (saved >= RESULTS_WANTED) break;

                    const productKey = product.product_id || product.product_url || product.name;
                    if (dedupe && productKey && seenProductIds.has(productKey)) continue;
                    if (productKey) seenProductIds.add(productKey);

                    // Add zipcode and store from URL if missing
                    product.zipcode = zipcode;
                    if (!product.store || product.store === 'Instacart') {
                        product.store = extractRetailerFromUrl(product.product_url || url);
                    }
                    if (!product.store_slug) {
                        product.store_slug = extractRetailerSlugFromUrl(product.product_url || url);
                    }

                    productsToSave.push(product);
                    allProducts.push(product);
                    saved++;
                }

                // Push listing data IMMEDIATELY as received
                if (productsToSave.length > 0) {
                    await Dataset.pushData(productsToSave);
                    log.info(`� Saved ${productsToSave.length} products | Total: ${saved}/${RESULTS_WANTED}`);
                }

                if (saved >= RESULTS_WANTED) break;
                currentPage++;
            }
        }

        // ==================== OPTIONAL: ENRICH WITH DETAIL PAGES ====================
        if (extractDetails) {
            // Filter products that need detail fetching
            const productsNeedingDetails = allProducts.filter(p => !p.price || !p.brand);

            if (productsNeedingDetails.length > 0) {
                log.info(`?? Fetching detail pages for ${productsNeedingDetails.length} products missing price/brand...`);

                const updatedProducts = [];
                const baseUrl = initial[0]?.url || startUrl;

                // HTTP/JSON-first detail fetch
                const httpUpdated = await fetchProductDetailsHTTP(productsNeedingDetails, baseUrl);
                updatedProducts.push(...httpUpdated);

                // Optional Playwright fallback if still missing details
                const remaining = productsNeedingDetails.filter(p => !p.price || !p.brand);
                if (detailPlaywrightFallback && remaining.length > 0) {
                    log.info(`?? Playwright fallback for ${remaining.length} products still missing details...`);
                    const pwUpdated = await fetchProductDetailsBatch(remaining);
                    updatedProducts.push(...pwUpdated);
                }

                // De-dupe updated products
                const uniqueUpdated = [];
                const updatedKeys = new Set();
                for (const product of updatedProducts) {
                    const key = product.product_id || product.product_url || product.name;
                    if (!key || updatedKeys.has(key)) continue;
                    updatedKeys.add(key);
                    uniqueUpdated.push(product);
                }

                if (uniqueUpdated.length > 0) {
                    await Dataset.pushData(uniqueUpdated.map(p => ({
                        ...p,
                        _enriched: true,
                        enriched_at: p.enriched_at || new Date().toISOString(),
                    })));
                    log.info(`? Enriched and saved ${uniqueUpdated.length} products with detail page data`);
                }
            } else {
                log.info(`? All products already have price/brand data`);
            }
        } else {
            log.info(`? Skipping detail page fetching (extractDetails is disabled)`);
        }

        // Summary
        log.info(`🎉 Completed! Saved ${saved} products from Instacart`);

        const stats = {
            total_products_saved: saved,
            target_results: RESULTS_WANTED,
            pages_processed: currentPage,
            zipcode: zipcode,
            used_playwright: usePlaywright,
            detail_concurrency: DETAIL_CONCURRENCY,
            detail_playwright_fallback: detailPlaywrightFallback,
            next_build_id: nextBuildId,
            extraction_methods: [...new Set(allProducts.map(p => p.extraction_method))],
        };

        await Actor.setValue('STATS', stats);
        log.info('📈 Stats saved to key-value store');

    } catch (error) {
        log.error('Actor failed');
        log.exception(error);
        throw error;
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
