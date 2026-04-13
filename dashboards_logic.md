# Plantex Sales Dashboard: Calculations & Logic Documentation

This document provides a clear, layman-friendly explanation of how metrics are calculated and what each chart/card represents across the three main dashboards: **CEO Dashboard**, **Business Dashboard**, and **Category Dashboard**.

---

## 📖 General Glossary

| Term | Meaning | Source Code (`analytics_services.py`) |
| :--- | :--- | :--- |
| **Revenue** | Total sales value generated from orders. | `revenue = float(df['revenue'].sum())` |
| **Orders** | Total number of unique orders placed. | `orders = int(df['orders'].sum())` |
| **Units** | Total items sold. | `units = int(df['units'].sum())` |
| **Ad Spend** | Total advertising investment. | `spend = float(df['total_spend'].sum())` |
| **Pageviews** | Total product detail page views. | `pageviews = int(df['pageviews'].sum())` |
| **ASIN / SKU** | A unique product identifier. | `active_asins = int(df['asin'].nunique())` |
| **Prev. Period** | The comparison timeframe. | *Logic handled in `apply_global_filters`* |

---

## 🧮 Core Calculations

These formulas are used consistently across all dashboards to drive KPI cards and charts.

### 1. Sales Efficiency
*   **ROAS (Return on Ad Spend)**: `Revenue ÷ Ad Spend`
    *   *Meaning*: For every ₹1 spent on ads, how many ₹ of revenue were generated.
    *   *Source*: `analytics_services.py`
    ```python
    roas = (revenue / spend) if spend > 0 else 0
    ```
*   **TACoS (Total Advertising Cost of Sale)**: `(Ad Spend ÷ Revenue) × 100`
    *   *Meaning*: What percentage of your total revenue is being spent on advertising.
    *   *Source*: `analytics_services.py`
    ```python
    tacos = (spend / revenue * 100) if revenue > 0 else 0
    ```
*   **CVR (Conversion Rate)**: `(Orders ÷ Pageviews) × 100`
    *   *Meaning*: What percentage of people who viewed your product actually bought it.
    *   *Source*: `analytics_services.py`
    ```python
    conversion = (orders / pageviews * 100) if pageviews > 0 else 0
    ```

### 2. Profitability
*   **AOV (Average Order Value)**: `Revenue ÷ Orders`
    *   *Meaning*: The average amount a customer spends in a single order.
    *   *Source*: `analytics_services.py`
    ```python
    aov = (revenue / orders) if orders > 0 else 0
    ```
*   **Gross Margin**: `Revenue - Cost of Goods Sold (COGS)`
    *   *Meaning*: Profit after accounting for product costs but before advertising.
    *   *Source*: `analytics_services.py`
    ```python
    gross_margin = revenue - cogs
    ```
*   **Gross Margin %**: `(Gross Margin ÷ Revenue) × 100`
    *   *Meaning*: Profitability percentage before ads.
    *   *Source*: `analytics_services.py`
    ```python
    gross_margin_pct = (gross_margin / revenue * 100) if revenue > 0 else 0
    ```
*   **Net Profit**: `Gross Margin - Ad Spend`
    *   *Meaning*: The money left over after paying for products and ads.
    *   *Source*: `analytics_services.py`
    ```python
    net_profit = gross_margin - spend
    ```
*   **Contribution Margin %**: `Gross Margin % - TACoS %`
    *   *Meaning*: A "health score" for profitability; how much each sale contributes after ads.
    *   *Source*: `analytics_services.py`
    ```python
    contribution_margin = round(gross_margin_pct - tacos, 1)
    ```

---

## 🏢 1. CEO Dashboard
*Focus: Overall Business Health, Growth, and Strategic Profitability.*

### KPI Cards
*   **Business Health Score**: A weighted score (0-100) based on Growth, Profitability, Inventory, and Operations.
*   **MOM Growth**: Month-over-Month growth in revenue compared to the previous 30 days.
*   **YOY Growth**: Year-over-Year growth (current period vs. same period last year).
*   **Run Rate vs Target**: Predicted revenue for the month based on current pace vs. the set goal.

### Charts & Tables
*   **Sales Trend**: Daily revenue vs. the same days in the previous period. Helps identify if you are trending up or down.
*   **Platform Split**: Donut chart showing revenue share from Amazon, Flipkart, etc.
*   **Critical Alerts**: Automated flags for issues like "High TACoS on Top Product" or "Sudden Revenue Drop."
*   **Revenue Forecast**: A statistical projection of where revenue will end by the end of the month.

---

## 📈 2. Business Dashboard
*Focus: Channel Performance, Inventory Management, and Marketing ROI.*

### KPI Cards
*   **Gross Margin %**: Percentage of revenue that is profit before marketing.
*   **Ad Spend**: Total investment in advertising across all platforms.
*   **Units Sold**: Volume of physical goods moved out of the warehouse.

### Charts & Tables
*   **Top 5 Categories by Growth**: Highlights which parts of the business are expanding the fastest.
    *   *Location*: `analytics_services.py:614`
*   **Inventory Health**:
    *   **In Stock**: Healthy SKU count.
    *   **Low Stock**: SKUs likely to run out in < 7 days (Orders > 0, low units).
    *   **OOS (Out of Stock)**: SKUs getting views but having 0 orders.
    *   *Source*: `analytics_services.py`
    ```python
    oos_mask = (asin_grp['pageviews'] > 20) & (asin_grp['orders'] == 0)
    low_stock_mask = (asin_grp['orders'] > 0) & (asin_grp['orders'] <= 3) & (asin_grp['pageviews'] > 50)
    overstock_mask = (asin_grp['units'] > 50) & (asin_grp['orders'] > 0) & (asin_grp['units'] > asin_grp['orders'] * 10)
    ```
*   **OOS Impact**: Calculates "Lost Revenue" by multiplying the typical daily sales of a product by the number of days it was out of stock.
    *   *Note*: Logic is applied via `apply_business_logic` around Line 495.
*   **Marketing Performance**: Breakdown of ROAS and TACoS across different ad types (SP, SB, SD).
    *   *Location*: `analytics_services.py:132-135`

---

## 🏷️ 3. Category Dashboard
*Focus: Deep-dive into Product Clusters, SKU performance, and Listing Quality.*

### KPI Cards
*   **Category Revenue**: Revenue specific to the selected category filter.
*   **Return Rate**: Percentage of units returned by customers.
*   **Active SKUs**: Number of unique products that had at least 1 order in the period.

### Charts & Tables
*   **Cluster Performance**: Breakdown of revenue and growth by "Subcategories" or "Clusters."
    *   *Location*: `analytics_services.py:634`
*   **Top/Underperforming Products**: List of products with the highest revenue and those with the largest percentage drop in sales.
    *   *Location*: `analytics_services.py:778-800`
*   **Inventory Position**: Bars showing revenue tied up in stock buckets (e.g., "60+ days of stock" vs "Critical < 7 days").
    *   *Location*: `analytics_services.py:685-709`
*   **Returns & Ratings**: Analysis of customer feedback. "Low Rating SKUs" are those with an average rating below 3.5 stars.
    *   *Note*: Values are placeholders (0) in the current logic at `analytics_services.py:747`.
*   **Growth Opportunities**: Identifies search trends or subcategories with high pageview growth but low current revenue, suggesting where to expand.
    *   *Location*: `analytics_services.py:756-770`

---

## ⚙️ Logic for "Days of Cover" (Inventory Position)
The dashboard calculates how many days your current stock will last using this logic:
`[(Current Units × 1.5) + (Daily Pageviews ÷ 10)] ÷ Average Daily Units Sold`

*Source*: `analytics_services.py`
```python
days_cover = ((row['units'] * 1.5) + (row['pageviews'] / 10.0)) / max(row['units'] / 30.0, 0.1)
```
*Rationale: We account for both actual sales and "pent-up demand" from pageviews to ensure we don't underestimate stock needs for high-traffic items.*
