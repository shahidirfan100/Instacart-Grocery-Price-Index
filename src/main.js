// Instacart Grocery Price Index - Production-ready scraper
// Uses JSON API extraction (Apollo GraphQL) as primary method with HTML parsing fallback
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
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
            extractDetails = true,
            zipcode = '94105', // Default to San Francisco
            proxyConfiguration,
            dedupe = true,
            delay_ms: DELAY_MS = 2000, // Stealth delay between requests
            user_agent_rotation: USER_AGENT_ROTATION = true
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 10;
        const DELAY_MS_VALUE = Number.isFinite(+DELAY_MS) ? Math.max(500, +DELAY_MS) : 2000;

        log.info(`Starting Instacart scraper with ${RESULTS_WANTED} results wanted, max ${MAX_PAGES} pages`);

        // Stealth user agents for rotation
        const USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ];

        const getHeaders = (useRotation = false) => {
            const baseHeaders = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
            };

            if (useRotation && USER_AGENT_ROTATION) {
                baseHeaders['User-Agent'] = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
            } else {
                baseHeaders['User-Agent'] = USER_AGENTS[0];
            }

            return baseHeaders;
        };

        const toAbs = (href, base) => {
            if (!href) return null;
            try { return new URL(href, base).href; } catch { return null; }
        };

        const parsePrice = (priceStr) => {
            if (!priceStr) return null;
            const cleaned = String(priceStr).replace(/[^0-9.]/g, '');
            const parsed = parseFloat(cleaned);
            return isNaN(parsed) ? null : parsed;
        };

        const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) {
            initial.push(...startUrls.map(u => typeof u === 'string' ? { url: u } : u));
        }
        if (startUrl) initial.push({ url: startUrl });
        if (!initial.length) initial.push({ url: 'https://www.instacart.com/categories/316-food/317-fresh-produce' });

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;
        let saved = 0;
        const seenUrls = new Set();
        const seenProductIds = new Set();

        // PRIMARY METHOD: Extract from Apollo GraphQL state (node-apollo-state)
        function extractApolloState($) {
            try {
                const apolloScript = $('script[id="node-apollo-state"]');
                if (apolloScript.length) {
                    let encodedData = apolloScript.html() || '';

                    // Decode HTML entities properly
                    const textarea = cheerioLoad('<textarea></textarea>');
                    textarea.find('textarea').html(encodedData);
                    const decodedData = textarea.find('textarea').text();

                    return JSON.parse(decodedData);
                }
            } catch (e) {
                log.debug('Failed to parse Apollo state:', e.message);
                log.debug('Apollo script content length:', $('script[id="node-apollo-state"]').html()?.length || 0);
            }
            return null;
        }

        // Extract products from Apollo GraphQL data
        function extractFromApolloState(apolloData) {
            const products = [];
            if (!apolloData) return products;

            try {
                // Look for LandingTaxonomyProducts query data
                const queries = apolloData.ROOT_QUERY || {};

                // Try multiple possible query keys (different pagination/params)
                const possibleKeys = [
                    'LandingTaxonomyProducts({"limit":48,"offset":0,"productCategoryId":"317"})',
                    'LandingTaxonomyProducts({"limit":24,"offset":0,"productCategoryId":"317"})',
                    'LandingTaxonomyProducts({"limit":48,"offset":0,"productCategoryId":"316"})',
                ];

                let taxonomyProducts = null;
                for (const key of possibleKeys) {
                    if (queries[key]) {
                        taxonomyProducts = queries[key];
                        log.debug(`Found Apollo query data with key: ${key}`);
                        break;
                    }
                }

                if (taxonomyProducts && taxonomyProducts.products && Array.isArray(taxonomyProducts.products)) {
                    log.info(`Found ${taxonomyProducts.products.length} products in Apollo state`);

                    for (const product of taxonomyProducts.products) {
                        if (!product || typeof product !== 'object') continue;

                        try {
                            const productData = {
                                product_id: product.id || product.productId || null,
                                name: product.name || product.title || null,
                                size: product.size || product.packageSize || null,
                                landing_param: product.landingParam || null,
                                image_url: product.image?.viewSection?.productImage?.url ||
                                          product.image?.url ||
                                          product.primaryImage?.url || null,
                                category: 'Fresh Produce',
                                store: 'Instacart',
                                in_stock: product.inStock !== false && product.availability?.status !== 'out_of_stock',
                                timestamp: new Date().toISOString(),
                                zipcode: zipcode,
                                extraction_method: 'apollo_graphql'
                            };

                            // Extract URL from landing param if available
                            if (productData.landing_param) {
                                productData.product_url = `https://www.instacart.com/products/${productData.landing_param}`;
                            }

                            // Only add products with at least a name or ID
                            if (productData.product_id || productData.name) {
                                products.push(productData);
                            }
                        } catch (productError) {
                            log.debug('Error processing individual product:', productError.message);
                            continue;
                        }
                    }

                    log.info(`Successfully processed ${products.length} valid products from Apollo data`);
                } else {
                    log.debug('No taxonomy products found in Apollo data');
                }
            } catch (e) {
                log.warning('Failed to extract from Apollo state:', e.message);
            }

            return products;
        }

        // FALLBACK METHOD: HTML parsing for products
        function extractFromHTML($, baseUrl) {
            const products = [];
            try {
                // Find product cards using data-testid or class selectors
                $('[data-testid*="product"], .product-card, .item-card').each((_, el) => {
                    const $el = $(el);

                    // Extract product name
                    const name = $el.find('[data-testid*="name"], .product-name, .item-name, h3, h4').first().text().trim();

                    // Extract price
                    const priceText = $el.find('[data-testid*="price"], .price, .current-price').first().text().trim();
                    const price = parsePrice(priceText);

                    // Extract image
                    const imgSrc = $el.find('img').attr('src') || $el.find('img').attr('data-src');

                    // Extract URL
                    const productUrl = $el.find('a').attr('href');
                    const fullUrl = productUrl ? toAbs(productUrl, baseUrl) : null;

                    if (name || price) {
                        products.push({
                            name: name || null,
                            price: price,
                            image_url: imgSrc ? toAbs(imgSrc, baseUrl) : null,
                            product_url: fullUrl,
                            category: 'Fresh Produce',
                            store: 'Instacart',
                            in_stock: true,
                            timestamp: new Date().toISOString(),
                            zipcode: zipcode,
                            extraction_method: 'html_parsing'
                        });
                    }
                });

                log.info(`Extracted ${products.length} products via HTML parsing fallback`);
            } catch (e) {
                log.debug('Failed HTML extraction:', e.message);
            }

            return products;
        }

        function findNextPage($, currentUrl) {
            // Look for pagination links
            const nextSelectors = [
                'a[rel="next"]',
                'a[data-testid*="next"]',
                'a[href*="?page="]',
                '.pagination a:last-child',
                '[aria-label*="Next"]'
            ];

            for (const selector of nextSelectors) {
                const nextLink = $(selector).last();
                if (nextLink.length) {
                    const href = nextLink.attr('href');
                    if (href) {
                        const nextUrl = toAbs(href, currentUrl);
                        log.debug(`Found next page: ${nextUrl}`);
                        return nextUrl;
                    }
                }
            }

            return null;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            useSessionPool: true,
            maxConcurrency: 2, // Lower concurrency for stealth
            requestHandlerTimeoutSecs: 60,
            additionalHttpHeaders: getHeaders(),

            preNavigationHooks: [
                async ({ request }) => {
                    // Add stealth delay
                    await sleep(DELAY_MS_VALUE);

                    // Rotate user agent if enabled
                    if (USER_AGENT_ROTATION) {
                        request.headers = { ...request.headers, ...getHeaders(true) };
                    }

                    log.debug(`Making request to: ${request.url}`);
                }
            ],

            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                crawlerLog.info(`Processing ${label} page ${pageNo}: ${request.url}`);

                let products = [];

                // PRIMARY: Try Apollo GraphQL extraction
                const apolloData = extractApolloState($);
                if (apolloData) {
                    products = extractFromApolloState(apolloData);
                    crawlerLog.info(`✅ Apollo GraphQL extraction successful: ${products.length} products`);
                }

                // FALLBACK: HTML parsing if Apollo failed
                if (products.length === 0) {
                    products = extractFromHTML($, request.url);
                    crawlerLog.info(`⚠️ Using HTML parsing fallback: ${products.length} products`);
                }

                // Deduplication
                if (dedupe) {
                    products = products.filter(p => {
                        const productId = p.product_id || p.product_url;
                        if (!productId || seenProductIds.has(productId)) return false;
                        seenProductIds.add(productId);
                        return true;
                    });
                }

                // Save products up to RESULTS_WANTED limit
                const remaining = RESULTS_WANTED - saved;
                const toSave = products.slice(0, Math.max(0, remaining));

                for (const product of toSave) {
                    await Dataset.pushData(product);
                    saved++;
                    crawlerLog.debug(`Saved product: ${product.name || product.product_id}`);
                }

                crawlerLog.info(`Total saved so far: ${saved}/${RESULTS_WANTED}`);

                // Continue to next page if needed
                if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                    const nextPageUrl = findNextPage($, request.url);
                    if (nextPageUrl) {
                        await enqueueLinks({
                            urls: [nextPageUrl],
                            userData: { label: 'LIST', pageNo: pageNo + 1 }
                        });
                        crawlerLog.info(`Enqueued next page: ${nextPageUrl}`);
                    } else {
                        crawlerLog.info('No more pages found');
                    }
                }

                // Stop if we've reached our target
                if (saved >= RESULTS_WANTED) {
                    crawlerLog.info(`Target reached: ${saved} products saved`);
                }
            },

            async failedRequestHandler({ request, error }) {
                log.warning(`Request failed for ${request.url}: ${error.message}`);
            }
        });

        // Set up initial requests
        const initialRequests = initial.map(u => ({
            ...(typeof u === 'string' ? { url: u } : u),
            userData: { label: 'LIST', pageNo: 1 }
        }));

        log.info(`Starting crawler with ${initialRequests.length} initial URLs`);
        await crawler.run(initialRequests);

        log.info(`🎉 Finished! Successfully saved ${saved} products from Instacart`);

        // Summary stats
        const stats = {
            total_products_saved: saved,
            target_results: RESULTS_WANTED,
            pages_processed: MAX_PAGES,
            zipcode: zipcode,
            extraction_methods_used: ['apollo_graphql', 'html_parsing'],
            stealth_features: {
                user_agent_rotation: USER_AGENT_ROTATION,
                request_delay_ms: DELAY_MS_VALUE,
                proxy_enabled: !!proxyConf
            }
        };

        await Actor.setValue('STATS', stats);
        log.info('Scraping stats saved to key-value store');

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
