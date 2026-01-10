// Instacart Grocery Price Index - Production-ready Apify Actor
// Hybrid approach: HTTP + Apollo GraphQL (Priority 1) → Playwright stealth fallback (Priority 2)
import { Actor, log } from 'apify';
import { CheerioCrawler, PlaywrightCrawler, Dataset } from 'crawlee';
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
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 10;
        const DELAY_MS_VALUE = Number.isFinite(+DELAY_MS) ? Math.max(1000, +DELAY_MS) : 2000;

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

        // Build initial URLs
        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) {
            initial.push(...startUrls.map(u => typeof u === 'string' ? { url: u } : u));
        }
        if (startUrl && !initial.some(u => u.url === startUrl)) initial.push({ url: startUrl });
        if (!initial.length) initial.push({ url: 'https://www.instacart.com/categories/316-food/317-fresh-produce' });

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

        /**
         * PRIORITY 1: Extract from Apollo GraphQL state (node-apollo-state)
         */
        function extractApolloState($) {
            try {
                const apolloScript = $('script#node-apollo-state');
                if (!apolloScript.length) {
                    log.info('⚠️ Apollo state script not found in page');
                    return null;
                }

                let rawData = apolloScript.html() || apolloScript.text() || '';
                log.info(`📊 Apollo script found, raw length: ${rawData.length} chars`);

                if (!rawData || rawData.length < 100) {
                    log.warning('Apollo state is empty or too short');
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
                    log.info(`✅ Apollo state parsed: ${keyCount} top-level keys`);

                    // Log sample keys for debugging
                    const sampleKeys = Object.keys(parsed).slice(0, 5);
                    log.debug(`Sample Apollo keys: ${sampleKeys.join(', ')}`);

                    return parsed;
                }

                log.warning('Apollo state does not contain valid JSON');
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

            return {
                product_id: id,
                name: name,
                size: size,
                image_url: imageUrl ? cleanImageUrl(imageUrl) : null,
                product_url: productUrl,
                in_stock: true,
                store: 'Instacart',
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
                store: 'Instacart',
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
         * PRIORITY 2: HTML parsing fallback
         */
        function extractFromHTML($, baseUrl) {
            const products = [];
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

            // Use closure variable to capture HTML from request handler
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

                async requestHandler({ page }) {
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
