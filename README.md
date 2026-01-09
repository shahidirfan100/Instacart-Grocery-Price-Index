# Instacart Grocery Price Index

Scrape and index grocery prices from Instacart for price comparison and market analysis.

## Overview

This Apify actor extracts product data from Instacart, including prices, descriptions, nutrition information, and customer reviews. It supports multiple extraction methods with automatic fallback for reliable data collection.

## Features

- **JSON API Extraction**: Primary method extracts structured data from Next.js hydration scripts
- **HTML Fallback**: Secondary method parses product data from HTML when JSON is unavailable
- **Product Details**: Optional deep scraping for nutrition facts, ingredients, and reviews
- **Price Tracking**: Captures current price, original price, and discount percentages
- **Multi-Category Support**: Scrape categories, subcategories, or search results
- **Location-Based**: Filter by store and zipcode for regional pricing
- **Pagination**: Automatic navigation through multiple pages of results
- **Deduplication**: Built-in URL deduplication to avoid duplicate products

## Use Cases

- **Price Comparison**: Compare prices across different grocery categories
- **Market Research**: Analyze product trends and pricing strategies
- **Budget Planning**: Track prices for cost-effective shopping
- **Competitor Analysis**: Monitor Instacart pricing across retailers
- **Data Enrichment**: Combine with other grocery data sources

## Input

Configure the scraper with the following parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `startUrl` | string | Instacart category URL | Starting URL to scrape |
| `startUrls` | array | - | Multiple URLs to scrape |
| `searchQuery` | string | - | Search term for finding products |
| `category` | string | - | Category ID for filtering |
| `subcategory` | string | - | Subcategory ID for filtering |
| `results_wanted` | number | 100 | Maximum products to collect |
| `max_pages` | number | 20 | Maximum pagination pages |
| `extractDetails` | boolean | true | Visit product pages for details |
| `includeNutrition` | boolean | true | Extract nutrition facts |
| `includeReviews` | boolean | true | Extract review counts and ratings |
| `storeId` | string | - | Filter by specific store |
| `zipcode` | string | - | Location for regional pricing |
| `dedupe` | boolean | true | Remove duplicate products |
| `proxyConfiguration` | object | Apify Proxy | Proxy settings |

### Example Input

```json
{
  "startUrl": "https://www.instacart.com/categories/316-food/317-fresh-produce",
  "results_wanted": 50,
  "max_pages": 5,
  "extractDetails": true,
  "includeNutrition": true,
  "includeReviews": true
}
```

### Search Example

```json
{
  "searchQuery": "organic bananas",
  "category": "317",
  "results_wanted": 25,
  "zipcode": "10001"
}
```

## Output

Each product is saved to the dataset with the following structure:

```json
{
  "product_id": "123456789",
  "name": "Organic Bananas",
  "brand": "Fresh Farms",
  "category": "Fresh Produce",
  "subcategory": "Fruit",
  "price": 1.99,
  "original_price": 2.49,
  "price_per_unit": 0.99,
  "unit": "lb",
  "size": "1 lb",
  "image_url": "https://example.com/image.jpg",
  "product_url": "https://www.instacart.com/store/products/123456789",
  "store": "Whole Foods Market",
  "in_stock": true,
  "discount_percent": 20,
  "rating": 4.5,
  "review_count": 128,
  "nutrition": {
    "serving_size": "1 medium banana (118g)",
    "calories": 105,
    "total_fat": "0.4g",
    "sodium": "1mg"
  },
  "ingredients": ["Organic Bananas"],
  "dietary_tags": ["organic", "vegan", "gluten-free"],
  "description": "Fresh organic bananas, perfect for snacking.",
  "timestamp": "2026-01-09T17:30:00.000Z",
  "zipcode": "10001"
}
```

### Output Views

- **Overview**: Key product information for quick comparison
- **Price Comparison**: Optimized view for price analysis
- **Detailed**: Full product data including nutrition and ingredients

## Getting Started

### Run Locally

```bash
npm install
npm start
```

### Run on Apify

1. Push to Apify:
   ```bash
   apify login
   apify push
   ```

2. Configure and run in Apify Console

### Docker

```bash
docker build -t instacart-grocery-price-index .
docker run instacart-grocery-price-index
```

## Best Practices

- Set reasonable `results_wanted` to avoid timeouts
- Use Apify Proxy for production runs
- Start with smaller `max_pages` values for testing
- Enable `dedupe` to avoid duplicate products
- Consider rate limiting for large crawls

## Rate Limiting

Respect Instacart's terms of service:

- Use reasonable concurrency settings
- Add delays between requests
- Cache results for repeated analysis
- Do not scrape for commercial purposes without permission

## Troubleshooting

### No Products Found

- Verify the start URL is accessible
- Check if Instacart has changed their structure
- Try enabling HTML fallback extraction
- Ensure cookies are not required

### Missing Data

- Enable `extractDetails` for full product pages
- Check if product pages require authentication
- Verify selectors match current site structure

### Rate Limit Errors

- Reduce `maxConcurrency` setting
- Enable proxy rotation
- Increase request delays
- Use residential proxies

## Output Formats

Access your data in multiple formats:

- **JSON**: `https://api.apify.com/v2/datasets/{datasetId}/items`
- **CSV**: `https://api.apify.com/v2/datasets/{datasetId}/items?format=csv`
- **Excel**: Download from Apify Console

## License

ISC

## Author

Shahid Irfan
