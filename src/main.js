// Instacart Grocery Price Index - Production-ready Apify Actor
// Hybrid approach: HTTP + Apollo GraphQL (Priority 1) → Playwright stealth fallback (Priority 2)
import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { gotScraping } from 'got-scraping';

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            startUrl = 'https://www.instacart.com/categories/316-food/317-fresh-produce',
            startUrls = [],
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 10,
            zipcode = '94105',
            proxyConfiguration,
            dedupe = true,
            delay_ms: DELAY_MS = 2000,
            extractDetails = true,
            detail_max_concurrency: DETAIL_CONCURRENCY_RAW = 2,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 10;
        const DELAY_MS_VALUE = Number.isFinite(+DELAY_MS) ? Math.max(1000, +DELAY_MS) : 2000;
        const DETAIL_CONCURRENCY = Number.isFinite(+DETAIL_CONCURRENCY_RAW)
            ? Math.min(6, Math.max(1, +DETAIL_CONCURRENCY_RAW))
            : 2;

        log.info(`🚀 Starting Instacart scraper | Target: ${RESULTS_WANTED} products | Max pages: ${MAX_PAGES}`);

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
        const safeJsonParse = (value) => {
            if (!value) return null;
            if (typeof value === 'object') return value;
            try { return JSON.parse(value); } catch { return null; }
        };

        const extractStoreSlugFromUrl = (url) => {
            if (!url) return null;
            try {
                const urlObj = new URL(url);
                const match = urlObj.pathname.match(/\/store\/([^\/]+)/);
                if (match && match[1]) return match[1];
            } catch { /* ignore */ }
            return null;
        };

        const storeNameFromSlug = (slug) => {
            if (!slug) return 'Instacart';
            return slug
                .split('-')
                .map(part => part.charAt(0).toUpperCase() + part.slice(1))
                .join(' ');
        };

        const extractProductIdFromUrl = (url) => {
            if (!url) return null;
            try {
                const urlObj = new URL(url);
                const match = urlObj.pathname.match(/\/products\/([^\/?#]+)/);
                if (match && match[1]) return match[1];
            } catch { /* ignore */ }
            return null;
        };

        const buildStoreProductUrl = (productUrl, storeSlug) => {
            if (!productUrl || !storeSlug) return productUrl;
            try {
                const productId = extractProductIdFromUrl(productUrl);
                if (productId) {
                    return `https://www.instacart.com/store/${storeSlug}/products/${productId}`;
                }
            } catch { /* ignore */ }
            return productUrl;
        };

        // Build initial URLs
        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) {
            initial.push(...startUrls.map(u => typeof u === 'string' ? { url: u } : u));
        }
        if (startUrl && !initial.some(u => u.url === startUrl)) initial.push({ url: startUrl });
        if (!initial.length) initial.push({ url: 'https://www.instacart.com/categories/316-food/317-fresh-produce' });

        const storeSlug = extractStoreSlugFromUrl(initial[0]?.url || startUrl);
        if (!storeSlug) {
            log.warning('?? Start URL is not store-specific. Prices/availability may be missing. Use /store/{slug}/... URLs.');
        }

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;
        let saved = 0;
        const seenProductIds = new Set();
        let usePlaywright = false;

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

        function parseApolloStateFromString(rawData) {
            if (!rawData || rawData.length < 100) return null;
            let decoded = rawData;
            if (decoded.includes('%7B') || decoded.includes('%22')) {
                try {
                    decoded = decodeURIComponent(decoded);
                    log.debug('URL decoding applied to Apollo state');
                } catch (e) {
                    log.debug('URL decoding not needed or failed, continuing...');
                }
            }

            const decodedData = decodeHtmlEntities(decoded);
            let jsonString = decodedData.trim();
            if (!jsonString.startsWith('{')) {
                const jsonMatch = jsonString.match(/\{[\s\S]*\}/);
                if (jsonMatch) jsonString = jsonMatch[0];
            }

            if (!jsonString.startsWith('{')) return null;
            return safeJsonParse(jsonString);
        }

        /**
         * PRIORITY 1: Extract from Apollo GraphQL state (node-apollo-state)
         */
        function extractApolloState($) {
            try {
                const apolloScript = $('script#node-apollo-state');
                if (!apolloScript.length) {
                    log.info('?? Apollo state script not found in page');
                    return null;
                }

                const rawData = apolloScript.html() || apolloScript.text() || '';
                log.info(`?? Apollo script found, raw length: ${rawData.length} chars`);

                const parsed = parseApolloStateFromString(rawData);
                if (!parsed) {
                    log.warning('Apollo state does not contain valid JSON');
                    return null;
                }

                const keyCount = Object.keys(parsed).length;
                log.info(`? Apollo state parsed: ${keyCount} top-level keys`);

                // Log sample keys for debugging
                const sampleKeys = Object.keys(parsed).slice(0, 5);
                log.debug(`Sample Apollo keys: ${sampleKeys.join(', ')}`);

                return parsed;
            } catch (e) {
                log.warning(`Apollo state parsing failed: ${e.message}`);
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
         * Extract product from Instacart's LandingLandingProduct structure
         */
        function extractLandingProduct(item, baseUrl) {
            if (!item || typeof item !== 'object') return {};

            const id = item.id || item.productId || null;
            const name = item.name || item.title || null;
            const size = item.size || null;
            const landingParam = item.landingParam || null;

            const priceSection = item.price?.viewSection || item.viewSection?.priceInfo || item.viewSection?.pricing || {};
            const itemCard = item.price?.viewSection?.itemCard || {};
            const itemDetails = item.price?.viewSection?.itemDetails || {};
            const priceRaw = itemCard.priceString || itemCard.price ||
                priceSection.priceString || priceSection.price ||
                item.priceString || item.price || null;
            const originalRaw = itemCard.plainFullPriceString || itemCard.fullPriceString ||
                itemCard.originalPrice || item.originalPrice || item.wasPrice ||
                priceSection.originalPrice || null;
            const unitPrice = itemDetails.pricePerUnitString ||
                itemDetails.unitPrice ||
                priceSection.pricePerUnitString ||
                item.unitPrice ||
                item.pricePerUnit ||
                null;
            const brand = item.brand || item.brandName || item.brandInfo?.name || null;
            const description = item.description ||
                item.productDescription ||
                item.shortDescription ||
                item.longDescription ||
                item.details?.description ||
                null;

            // Extract image URL from nested structure
            let imageUrl = null;
            const templateUrl = item.image?.viewSection?.productImage?.templateUrl ||
                item.image?.url ||
                item.imageUrl ||
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

            const slugFromBase = extractStoreSlugFromUrl(baseUrl);
            const storeSlugValue = item.retailerSlug || item.storeSlug || slugFromBase;
            const store = item.retailerName || item.storeName || item.retailer?.name || item.store?.name || storeNameFromSlug(storeSlugValue);

            return {
                product_id: id,
                name: name,
                brand: brand,
                price: typeof priceRaw === 'number' ? priceRaw : parsePrice(priceRaw),
                original_price: typeof originalRaw === 'number' ? originalRaw : parsePrice(originalRaw),
                unit_price: unitPrice,
                size: size,
                image_url: imageUrl ? cleanImageUrl(imageUrl) : null,
                product_url: productUrl,
                description: description,
                in_stock: true,
                store: store,
                store_slug: storeSlugValue,
                category: 'Fresh Produce',
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

            const size = product.size || product.packageSize || product.unitSize || null;
            const brand = product.brand || product.brandName || null;
            const description = product.description || product.productDescription || product.longDescription || null;
            const inStock = product.inStock !== false &&
                product.availability?.status !== 'out_of_stock' &&
                product.isAvailable !== false;
            const slugFromBase = extractStoreSlugFromUrl(baseUrl);
            const storeSlugValue = product.storeSlug || product.retailerSlug || slugFromBase;
            const store = product.storeName || product.retailerName || product.store?.name || storeNameFromSlug(storeSlugValue);

            return {
                product_id: id,
                name: name,
                brand: brand,
                price: typeof price === 'number' ? price : parsePrice(price),
                original_price: typeof originalPrice === 'number' ? originalPrice : parsePrice(originalPrice),
                description: description,
                size: size,
                image_url: imageUrl ? cleanImageUrl(toAbs(imageUrl, baseUrl)) : null,
                product_url: productUrl ? toAbs(productUrl, baseUrl) : null,
                in_stock: inStock,
                store: store,
                store_slug: storeSlugValue,
                timestamp: new Date().toISOString(),
                extraction_method: 'apollo_graphql'
            };
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

        function findStoreInObject(obj, depth = 0) {
            if (!obj || depth > 6) return null;
            if (Array.isArray(obj)) {
                for (const item of obj) {
                    const found = findStoreInObject(item, depth + 1);
                    if (found) return found;
                }
                return null;
            }
            if (typeof obj !== 'object') return null;

            if (obj.__typename === 'Retailer' || obj.__typename === 'Store') {
                const name = obj.name || obj.displayName;
                if (name) {
                    return {
                        store: name,
                        store_slug: obj.slug || obj.retailerSlug || obj.storeSlug || null,
                    };
                }
            }

            for (const value of Object.values(obj)) {
                const found = findStoreInObject(value, depth + 1);
                if (found) return found;
            }
            return null;
        }

        function extractDetailFromApolloData(apolloData, product, baseUrl) {
            if (!apolloData || typeof apolloData !== 'object') return null;
            const productId = product?.product_id || extractProductIdFromUrl(product?.product_url);

            if (productId) {
                const directItem = apolloData[`Item:${productId}`];
                if (directItem) return extractLandingProduct(directItem, baseUrl);
                const directProduct = apolloData[`Product:${productId}`];
                if (directProduct) return extractProductFields(directProduct, baseUrl);
            }

            const item = findProductInObject(apolloData, productId);
            if (item) {
                if (item.price?.viewSection) return extractLandingProduct(item, baseUrl);
                return extractProductFields(item, baseUrl);
            }

            const candidates = extractProductsFromApollo(apolloData, baseUrl);
            if (productId) {
                return candidates.find(p => String(p.product_id) === String(productId)) || null;
            }
            if (product?.product_url) {
                return candidates.find(p => p.product_url === product.product_url) || null;
            }
            return candidates[0] || null;
        }

        function extractDetailFromApiPayload(payload, product, baseUrl) {
            if (!payload) return null;
            const container = payload?.data || payload;
            const productId = product?.product_id || extractProductIdFromUrl(product?.product_url);
            const productObj = findProductInObject(container, productId);
            if (!productObj) return null;

            const detail = productObj.price?.viewSection
                ? extractLandingProduct(productObj, baseUrl)
                : extractProductFields(productObj, baseUrl);
            const storeInfo = findStoreInObject(container);
            if (storeInfo) {
                if (!detail.store && storeInfo.store) detail.store = storeInfo.store;
                if (!detail.store_slug && storeInfo.store_slug) detail.store_slug = storeInfo.store_slug;
            }
            detail.extraction_method = 'api_intercept';
            return detail;
        }

        function mergeProductDetails(target, detail) {
            if (!target || !detail) return false;
            let updated = false;
            const fields = [
                'product_id',
                'price',
                'original_price',
                'unit_price',
                'brand',
                'description',
                'size',
                'image_url',
                'product_url',
                'in_stock',
                'store',
                'store_slug',
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

            if (updated && detail.extraction_method) {
                target.detail_extraction_method = detail.extraction_method;
                target.enriched_at = new Date().toISOString();
            }
            return updated;
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
         * PRIORITY 2: HTML parsing fallback
         */
        function extractFromHTML($, baseUrl) {
            const products = [];
            try {
                const slugFromBase = extractStoreSlugFromUrl(baseUrl);
                const storeName = storeNameFromSlug(slugFromBase);
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
                                store: storeName,
                                store_slug: slugFromBase,
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
            return products;
        }

        // ==================== HTTP REQUEST METHOD (PRIORITY 1) ====================

        async function fetchWithHTTP(url) {
            try {
                const response = await gotScraping({
                    url,
                    headers: {
                        'User-Agent': getRandomUA(),
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Cache-Control': 'no-cache',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                    },
                    timeout: { request: 30000 },
                    retry: { limit: 2 },
                    proxyUrl: proxyConf ? await proxyConf.newUrl() : undefined,
                });

                if (response.statusCode === 200) {
                    return response.body;
                }

                log.warning(`HTTP request returned status ${response.statusCode}`);
                return null;
            } catch (e) {
                log.warning(`HTTP request failed: ${e.message}`);
                return null;
            }
        }

        // ==================== PLAYWRIGHT FALLBACK (PRIORITY 2) ====================

        async function fetchWithPlaywright(url) {
            log.info(`🎭 Using Playwright stealth mode for: ${url}`);

            const playwrightCrawler = new PlaywrightCrawler({
                proxyConfiguration: proxyConf,
                maxRequestRetries: 2,
                requestHandlerTimeoutSecs: 60,
                headless: true,

                launchContext: {
                    launchOptions: {
                        args: [
                            '--disable-blink-features=AutomationControlled',
                            '--disable-dev-shm-usage',
                            '--no-sandbox',
                        ],
                    },
                },

                preNavigationHooks: [
                    async ({ page }) => {
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
                    await page.waitForTimeout(2000);

                    // Get page HTML
                    const html = await page.content();
                    request.userData.html = html;
                },
            });

            const result = { html: null };

            try {
                await playwrightCrawler.run([{
                    url,
                    userData: { label: 'PLAYWRIGHT' },
                }]);

                // Get the result from the last request
                const requestQueue = await playwrightCrawler.requestQueue;
                if (requestQueue) {
                    const { items } = await requestQueue.getHandledRequests();
                    if (items.length > 0) {
                        result.html = items[0].userData?.html;
                    }
                }
            } catch (e) {
                log.warning(`Playwright crawl failed: ${e.message}`);
            }

            return result.html;
        }

        // ==================== PLAYWRIGHT DETAIL ENRICHMENT ====================

        async function fetchDetailsWithPlaywright(products) {
            if (!products.length) return [];
            usePlaywright = true;
            log.info(`?? Fetching detail pages via Playwright (${products.length} products)...`);

            const updatedProducts = [];

            const playwrightCrawler = new PlaywrightCrawler({
                proxyConfiguration: proxyConf,
                maxRequestRetries: 1,
                requestHandlerTimeoutSecs: 60,
                headless: true,
                maxConcurrency: DETAIL_CONCURRENCY,
                useSessionPool: true,
                persistCookiesPerSession: true,
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
                            '--disable-gpu',
                            '--disable-software-rasterizer',
                        ],
                    },
                },
                preNavigationHooks: [
                    async ({ page, request }) => {
                        request.userData.responsePayloads = [];

                        await page.route('**/*', (route) => {
                            const resourceType = route.request().resourceType();
                            if (['image', 'media', 'font', 'stylesheet'].includes(resourceType)) {
                                return route.abort();
                            }
                            return route.continue();
                        });

                        page.on('response', async (response) => {
                            try {
                                const req = response.request();
                                const resourceType = req.resourceType();
                                if (!['xhr', 'fetch'].includes(resourceType)) return;

                                const contentType = response.headers()['content-type'] || '';
                                if (!contentType.includes('application/json')) return;

                                const url = response.url();
                                if (!url.includes('graphql') && !url.includes('/api/') && !url.includes('/v3')) return;

                                const payload = await response.json().catch(() => null);
                                if (!payload) return;

                                const list = request.userData.responsePayloads;
                                if (Array.isArray(list) && list.length < 12) {
                                    list.push(payload);
                                }
                            } catch {
                                // Ignore response parsing issues
                            }
                        });

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
                    const product = request.userData.product;
                    const detailUrl = request.url;

                    await page.waitForLoadState('domcontentloaded');
                    await page.waitForTimeout(1200);

                    let updated = false;

                    const apolloRaw = await page.$eval('#node-apollo-state', el => el.textContent).catch(() => null);
                    if (apolloRaw) {
                        const apolloData = parseApolloStateFromString(apolloRaw);
                        const detail = extractDetailFromApolloData(apolloData, product, detailUrl);
                        if (detail) {
                            detail.extraction_method = 'apollo_detail';
                            updated = mergeProductDetails(product, detail) || updated;
                        }
                    }

                    const payloads = request.userData.responsePayloads || [];
                    for (const payload of payloads) {
                        const detail = extractDetailFromApiPayload(payload, product, detailUrl);
                        if (detail) {
                            updated = mergeProductDetails(product, detail) || updated;
                        }
                    }

                    if (updated) {
                        updatedProducts.push(product);
                    }
                },
            });

            const requests = products.map((product) => {
                const baseUrl = product.product_url ||
                    (product.product_id ? `https://www.instacart.com/products/${product.product_id}` : '');
                const productUrl = buildStoreProductUrl(baseUrl, storeSlug);
                if (!product.product_url) product.product_url = productUrl;
                return {
                    url: productUrl,
                    userData: { product },
                    uniqueKey: productUrl,
                };
            }).filter(r => r.url);

            await playwrightCrawler.run(requests);

            log.info(`?? Detail enrichment updated ${updatedProducts.length}/${products.length} products`);
            return updatedProducts;
        }

        // ==================== MAIN SCRAPING LOGIC ====================

        async function scrapeUrl(url, pageNo = 1) {
            const products = [];

            // Add stealth delay
            if (pageNo > 1) {
                await sleep(DELAY_MS_VALUE);
            }

            log.info(`📥 Processing page ${pageNo}: ${url}`);

            // Try HTTP first (Priority 1)
            let html = await fetchWithHTTP(url);

            // If HTTP fails, use Playwright (Priority 2)
            if (!html && !usePlaywright) {
                log.info('⚠️ HTTP failed, switching to Playwright mode');
                usePlaywright = true;
            }

            if (!html && usePlaywright) {
                html = await fetchWithPlaywright(url);
            }

            if (!html) {
                log.error(`❌ Failed to fetch: ${url}`);
                return products;
            }

            // Parse HTML with Cheerio
            const $ = cheerioLoad(html);

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

                // Deduplicate and save
                for (const product of products) {
                    if (saved >= RESULTS_WANTED) break;

                    const productKey = product.product_id || product.product_url || product.name;
                    if (dedupe && productKey && seenProductIds.has(productKey)) continue;
                    if (productKey) seenProductIds.add(productKey);

                    // Add zipcode
                    product.zipcode = zipcode;

                    allProducts.push(product);
                    saved++;
                }

                log.info(`📊 Progress: ${saved}/${RESULTS_WANTED} products saved`);

                if (saved >= RESULTS_WANTED) break;
                currentPage++;
            }
        }

        if (extractDetails) {
            const needsDetails = allProducts.filter(p =>
                !p.price || !p.brand || !p.description || !p.store || p.store === 'Instacart'
            );
            if (needsDetails.length > 0) {
                log.info(`?? Enriching ${needsDetails.length} products with detail pages...`);
                await fetchDetailsWithPlaywright(needsDetails);
            }
        }

        // Push all data in batches
        const BATCH_SIZE = 20;
        for (let i = 0; i < allProducts.length; i += BATCH_SIZE) {
            const batch = allProducts.slice(i, i + BATCH_SIZE);
            await Dataset.pushData(batch);
            log.info(`💾 Pushed batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(allProducts.length / BATCH_SIZE)}`);
        }

        // Summary
        log.info(`🎉 Completed! Saved ${saved} products from Instacart`);

        const stats = {
            total_products_saved: saved,
            target_results: RESULTS_WANTED,
            pages_processed: currentPage,
            zipcode: zipcode,
            used_playwright: usePlaywright,
            extraction_methods: [...new Set(allProducts.map(p => p.extraction_method))],
        };

        await Actor.setValue('STATS', stats);
        log.info('📈 Stats saved to key-value store');

    } catch (error) {
        log.error('Actor failed:', error);
        throw error;
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
