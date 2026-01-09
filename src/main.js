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
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 20,
            extractDetails = true,
            includeNutrition = true,
            includeReviews = true,
            zipcode,
            proxyConfiguration,
            cookies,
            dedupe = true
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 20;

        // Stealth headers
        const getHeaders = () => ({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            ...(cookies ? { 'Cookie': cookies } : {}),
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

        // Initialize start URLs
        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) {
            initial.push(...startUrls.map(u => typeof u === 'string' ? { url: u } : u));
        }
        if (startUrl) {
            initial.push({ url: startUrl });
        }
        if (!initial.length) {
            initial.push({ url: 'https://www.instacart.com/categories/316-food/317-fresh-produce' });
        }

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

        let saved = 0;
        const seenUrls = new Set();

        // Extract __NEXT_DATA__ JSON
        function extractNextData($) {
            try {
                const nextDataScript = $('script[id="__NEXT_DATA__"]');
                if (nextDataScript.length) {
                    return JSON.parse(nextDataScript.html() || '{}');
                }
            } catch (e) { log.debug('Failed to parse __NEXT_DATA__'); }
            return null;
        }

        // Extract products from JSON-LD Schema.org
        function extractJsonLd($) {
            const products = [];
            $('script[type="application/ld+json"]').each((_, el) => {
                try {
                    const content = $(el).html();
                    if (!content) return;
                    const data = JSON.parse(content);
                    const items = data['@graph'] || (Array.isArray(data) ? data : [data]);
                    
                    for (const item of items) {
                        if (!item) continue;
                        const type = item['@type'] || '';
                        if (type === 'Product' || type === 'https://schema.org/Product') {
                            products.push({
                                product_id: item.sku || item.gtin13 || null,
                                name: item.name || null,
                                brand: item.brand?.name || item.brand || null,
                                description: item.description || null,
                                image_url: Array.isArray(item.image) ? item.image[0] : item.image || null,
                                price: parsePrice(item.offers?.price),
                                original_price: parsePrice(item.offers?.priceSpecification?.price),
                                in_stock: item.offers?.availability?.includes('InStock'),
                                rating: item.aggregateRating?.ratingValue || null,
                                review_count: item.aggregateRating?.reviewCount || null,
                            });
                        }
                    }
                } catch (e) { log.debug('Failed to parse JSON-LD'); }
            });
            return products;
        }

        // Deep extraction from __NEXT_DATA__
        function extractFromNextData(nextData) {
            const products = [];
            if (!nextData) return products;
            
            try {
                const pageProps = nextData.props?.pageProps || {};
                const dataPaths = [
                    pageProps.products, pageProps.items, pageProps.productTiles,
                    pageProps.searchResults, pageProps.catalogResults, pageProps.data
                ];

                for (const dataPath of dataPaths) {
                    if (!dataPath) continue;
                    const items = Array.isArray(dataPath) ? dataPath : (dataPath.products || dataPath.items || dataPath.results || []);
                    
                    for (const item of items) {
                        if (!item) continue;
                        const product = item.product || item.item || item;
                        
                        products.push({
                            product_id: product.id || product.productId || product.tcin || product.sku || null,
                            name: product.name || product.title || null,
                            brand: product.brand || product.brandName || product.manufacturer || null,
                            category: product.category || product.department || null,
                            subcategory: product.subcategory || product.subDepartment || null,
                            price: parsePrice(product.price || product.currentPrice || product.retailPrice),
                            original_price: parsePrice(product.wasPrice || product.originalPrice || product.listPrice),
                            price_per_unit: parsePrice(product.pricePerUnit || product.unitPrice),
                            unit: product.unit || product.sizeUnit || null,
                            size: product.size || product.packageSize || product.quantity || null,
                            image_url: product.imageUrl || product.image || product.thumbnail || null,
                            product_url: product.url || product.productUrl || product.link || null,
                            store: product.storeName || product.retailer || null,
                            in_stock: product.inStock !== false && product.outOfStock !== true,
                            discount_percent: product.discountPercent || product.salePercentage || null,
                            rating: product.rating || product.averageRating || null,
                            review_count: product.reviewCount || product.reviewsCount || null,
                            nutrition: product.nutrition || null,
                            ingredients: product.ingredients || null,
                            dietary_tags: product.dietaryTags || product.tags || [],
                            description: product.description || product.shortDescription || null,
                        });
                    }
                    if (products.length > 0) break;
                }
            } catch (e) { log.debug('Failed to extract from __NEXT_DATA__'); }
            return products;
        }

        // Parse products from HTML fallback
        function parseProductsFromHtml($, baseUrl) {
            const products = [];

            // Data attributes
            $('[data-item], [data-product]').each((_, el) => {
                const $el = $(el);
                const dataAttr = $el.attr('data-item') || $el.attr('data-product');
                if (dataAttr) {
                    try {
                        const data = JSON.parse(dataAttr);
                        const item = data.product || data.item || data;
                        products.push({
                            product_id: item.id || item.productId || null,
                            name: item.name || null,
                            brand: item.brand || null,
                            price: parsePrice(item.price),
                            original_price: parsePrice(item.originalPrice),
                            image_url: $el.find('img').attr('src') || null,
                            product_url: toAbs($el.find('a[href*="/product/"]').attr('href'), baseUrl),
                            in_stock: !$el.find('[data-qa="out-of-stock"]').length,
                        });
                    } catch (e) { }
                }
            });

            // Generic selectors
            if (products.length === 0) {
                $('[class*="product-card"], [class*="productTile"]').each((_, el) => {
                    const $el = $(el);
                    products.push({
                        product_id: $el.attr('data-product-id') || null,
                        name: $el.find('[class*="name"], [class*="title"]').first().text().trim() || $el.find('img[alt]').attr('alt') || null,
                        brand: $el.find('[class*="brand"]').first().text().trim() || null,
                        price: parsePrice($el.find('[class*="price"]').first().text()),
                        image_url: $el.find('img').attr('src') || $el.find('img').attr('data-src') || null,
                        product_url: toAbs($el.find('a[href]').attr('href'), baseUrl),
                        in_stock: !$el.find('[class*="out-of-stock"]').length,
                    });
                });
            }

            return products;
        }

        // Extract product details from detail page
        async function extractProductDetails(url, product) {
            try {
                const response = await gotScraping.get(url, {
                    headers: getHeaders(),
                    proxyUrl: proxyConf?.newUrl(),
                    timeout: { request: 30000 },
                    throwHttpErrors: false,
                });

                const $ = cheerioLoad(response.body);
                
                // Priority 1: __NEXT_DATA__
                const nextData = extractNextData($);
                if (nextData) {
                    const nextProducts = extractFromNextData(nextData);
                    if (nextProducts.length > 0) {
                        return { ...product, ...nextProducts[0], description: product.description || nextProducts[0].description };
                    }
                }

                // Priority 2: JSON-LD
                const jsonLdProducts = extractJsonLd($);
                if (jsonLdProducts.length > 0) {
                    return { ...product, ...jsonLdProducts[0], description: product.description || jsonLdProducts[0].description };
                }

                // Priority 3: HTML fallback
                const description = $('[data-qa="product-description"], .product-description').text().trim();
                let nutrition = null;
                if (includeNutrition) {
                    const nutritionTable = $('[data-qa="nutrition"], .nutrition-facts');
                    if (nutritionTable.length) nutrition = { raw: nutritionTable.text().trim() };
                }
                const ingredients = includeNutrition ? $('[data-qa="ingredients"], .ingredients').text().split(/[,;]/).map(i => i.trim()).filter(Boolean) : [];
                const ratingText = $('[data-qa="rating-value"], .rating-value').attr('aria-label') || $('[data-qa="rating-value"]').text();
                const rating = ratingText ? parseFloat(ratingText.match(/[\d.]+/)?.[0] || '') || null : null;
                const reviewCountText = $('[data-qa="review-count"], .review-count').text();
                const reviewCount = reviewCountText ? parseInt(reviewCountText.replace(/[^0-9]/g, '') || '0') : null;

                return {
                    ...product,
                    description: product.description || description,
                    nutrition,
                    ingredients,
                    dietary_tags: [...new Set(product.dietary_tags || [])],
                    rating: product.rating || rating,
                    review_count: product.review_count || reviewCount,
                };
            } catch (err) {
                log.warning(`Failed to extract details for ${url}: ${err.message}`);
                return product;
            }
        }

        // Find next page
        function findNextPage($, currentUrl) {
            const nextBtn = $('[data-qa="next-page"], [data-testid="next"], .pagination-next, a[rel="next"]');
            if (nextBtn.length) {
                const href = nextBtn.attr('href') || nextBtn.find('a').attr('href');
                if (href) return toAbs(href, currentUrl);
            }
            const nextLink = $('a').filter((_, el) => /next|›|»|>|page \d+/i.test($(el).text().toLowerCase().trim()) && $(el).attr('href')).first();
            if (nextLink.length) return toAbs(nextLink.attr('href'), currentUrl);
            const pageMatch = currentUrl.match(/[?&]page=(\d+)/);
            if (pageMatch) return currentUrl.replace(/[?&]page=\d+/, `?page=${parseInt(pageMatch[1]) + 1}`);
            return null;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            useSessionPool: true,
            maxConcurrency: 5,
            requestHandlerTimeoutSecs: 60,
            additionalHttpHeaders: getHeaders(),
            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    crawlerLog.info(`Processing page ${pageNo}: ${request.url}`);

                    // Priority 1: __NEXT_DATA__
                    let products = [];
                    const nextData = extractNextData($);
                    if (nextData) {
                        products = extractFromNextData(nextData);
                        log.info(`Extracted ${products.length} products from __NEXT_DATA__`);
                    }

                    // Priority 2: JSON-LD
                    if (products.length === 0) {
                        products = extractJsonLd($);
                        log.info(`Extracted ${products.length} products from JSON-LD`);
                    }

                    // Priority 3: HTML parsing
                    if (products.length === 0) {
                        products = parseProductsFromHtml($, request.url);
                        log.info(`Extracted ${products.length} products from HTML`);
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
                        const enrichedProduct = {
                            ...product,
                            product_url: product.product_url || request.url,
                            timestamp: new Date().toISOString(),
                            zipcode: zipcode || null,
                        };

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
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    const product = request.userData?.product || {};
                    try {
                        const enriched = await extractProductDetails(request.url, product);
                        await Dataset.pushData(enriched);
                        saved++;
                        crawlerLog.info(`DETAIL ${request.url} -> saved product`);
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

