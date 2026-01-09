// Instacart Grocery Price Index - Production-ready scraper
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
            results_wanted: RESULTS_WANTED_RAW = 50,
            max_pages: MAX_PAGES_RAW = 5,
            extractDetails = true,
            zipcode,
            proxyConfiguration,
            dedupe = true
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 50;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 5;

        const getHeaders = () => ({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        });

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

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) {
            initial.push(...startUrls.map(u => typeof u === 'string' ? { url: u } : u));
        }
        if (startUrl) initial.push({ url: startUrl });
        if (!initial.length) initial.push({ url: 'https://www.instacart.com/categories/316-food/317-fresh-produce' });

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;
        let saved = 0;
        const seenUrls = new Set();

        function extractNextData($) {
            try {
                const nextDataScript = $('script[id="__NEXT_DATA__"]');
                if (nextDataScript.length) {
                    return JSON.parse(nextDataScript.html() || '{}');
                }
            } catch (e) { log.debug('Failed to parse __NEXT_DATA__'); }
            return null;
        }

        function extractFromNextData(nextData) {
            const products = [];
            if (!nextData) return products;
            try {
                const pageProps = nextData.props?.pageProps || {};
                const dataPaths = [pageProps.products, pageProps.items, pageProps.productTiles, pageProps.searchResults, pageProps.data];
                for (const dataPath of dataPaths) {
                    if (!dataPath) continue;
                    const items = Array.isArray(dataPath) ? dataPath : (dataPath.products || dataPath.items || dataPath.results || []);
                    for (const item of items) {
                        if (!item) continue;
                        const product = item.product || item.item || item;
                        products.push({
                            product_id: product.id || product.productId || product.tcin || null,
                            name: product.name || product.title || null,
                            brand: product.brand || product.brandName || null,
                            category: product.category || product.department || null,
                            price: parsePrice(product.price || product.currentPrice),
                            original_price: parsePrice(product.originalPrice || product.wasPrice),
                            image_url: product.imageUrl || product.image || null,
                            product_url: product.url || product.productUrl || null,
                            store: product.storeName || product.retailer || null,
                            in_stock: product.inStock !== false,
                            discount_percent: product.discountPercent || null,
                        });
                    }
                    if (products.length > 0) break;
                }
            } catch (e) { log.debug('Failed to extract from __NEXT_DATA__'); }
            return products;
        }

        function findProductLinks($, baseUrl) {
            const links = [];
            $('a[href*="/product/"], a[href*="/store/products/"]').each((_, el) => {
                const href = $(el).attr('href');
                const url = toAbs(href, baseUrl);
                if (url && !seenUrls.has(url)) {
                    links.push(url);
                    seenUrls.add(url);
                }
            });
            return links;
        }

        async function extractProductDetails(url, product) {
            try {
                const response = await gotScraping.get(url, {
                    headers: getHeaders(),
                    proxyUrl: proxyConf?.newUrl(),
                    timeout: { request: 30000 },
                    throwHttpErrors: false,
                });
                const $ = cheerioLoad(response.body);
                const nextData = extractNextData($);
                if (nextData) {
                    const nextProducts = extractFromNextData(nextData);
                    if (nextProducts.length > 0) return { ...product, ...nextProducts[0] };
                }
                return product;
            } catch (err) {
                log.warning(`Failed to extract details for ${url}: ${err.message}`);
                return product;
            }
        }

        function findNextPage($, currentUrl) {
            const nextBtn = $('[data-qa="next-page"], a[rel="next"]');
            if (nextBtn.length) {
                const href = nextBtn.attr('href') || nextBtn.find('a').attr('href');
                if (href) return toAbs(href, currentUrl);
            }
            return null;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            useSessionPool: true,
            maxConcurrency: 3,
            requestHandlerTimeoutSecs: 60,
            additionalHttpHeaders: getHeaders(),
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                crawlerLog.info(`Processing ${label} page ${pageNo}: ${request.url}`);

                if (label === 'LIST') {
                    let products = [];
                    const nextData = extractNextData($);
                    if (nextData) {
                        products = extractFromNextData(nextData);
                        crawlerLog.info(`Extracted ${products.length} products from __NEXT_DATA__`);
                    }
                    if (products.length === 0) {
                        const links = findProductLinks($, request.url);
                        crawlerLog.info(`Found ${links.length} product links to scrape`);
                        if (links.length > 0) {
                            const remaining = RESULTS_WANTED - saved;
                            const toEnqueue = links.slice(0, Math.max(0, remaining));
                            for (const link of toEnqueue) {
                                await Dataset.pushData({
                                    product_url: link,
                                    name: null,
                                    price: null,
                                    timestamp: new Date().toISOString(),
                                    zipcode: zipcode || null,
                                });
                                saved++;
                            }
                        }
                    }

                    if (dedupe) {
                        products = products.filter(p => {
                            const url = p.product_url;
                            if (!url || seenUrls.has(url)) return false;
                            seenUrls.add(url);
                            return true;
                        });
                    }

                    const remaining = RESULTS_WANTED - saved;
                    const toPush = products.slice(0, Math.max(0, remaining));

                    for (const product of toPush) {
                        const enrichedProduct = { ...product, timestamp: new Date().toISOString(), zipcode: zipcode || null };
                        if (extractDetails && product.product_url && product.product_url !== request.url) {
                            if (saved < RESULTS_WANTED) {
                                await enqueueLinks({ urls: [product.product_url], userData: { label: 'DETAIL', product: enrichedProduct } });
                            }
                        } else {
                            await Dataset.pushData(enrichedProduct);
                            saved++;
                        }
                    }

                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url);
                        if (next) await enqueueLinks({ urls: [next], userData: { label: 'LIST', pageNo: pageNo + 1 } });
                    }
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    const product = request.userData?.product || {};
                    try {
                        const enriched = await extractProductDetails(request.url, product);
                        await Dataset.pushData(enriched);
                        saved++;
                    } catch (err) {
                        crawlerLog.error(`DETAIL ${request.url} failed: ${err.message}`);
                        await Dataset.pushData(product);
                        saved++;
                    }
                }
            }
        });

        await crawler.run(initial.map(u => typeof u === 'string' ? { url: u, userData: { label: 'LIST', pageNo: 1 } } : { ...u, userData: { label: 'LIST', pageNo: 1 } }));
        log.info(`Finished. Saved ${saved} products`);
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });
