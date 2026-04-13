# Plantex Sales Dashboard: User Guide

Welcome to the Plantex Sales Dashboard user guide. This document is organized by dashboard to help you understand exactly what you are seeing on each page, what the metrics mean, and how they are calculated.

---

## 🏢 1. CEO Dashboard
**Target Audience**: CEO Office & Executive Leadership
**Focus**: Overall Business Health, Strategy, and Growth.

### 📊 KPI Cards (Key Metrics)
*   **Total Revenue**: Total value of sales.
    *   *Calculation*: Sum of all sales revenue.
*   **MOM Growth**: Growth compared to the previous 30 days.
    *   *Calculation*: `[(Current Sales - Previous Sales) ÷ Previous Sales] × 100`
*   **Health Score**: A grade (0-100) for overall business performance.
    *   *Calculation*: Weighted average of Growth, Profit, Inventory, and Operations.
*   **Net Profit**: Money left after paying for products and ads.
    *   *Calculation*: `(Revenue - Product Cost) - Ad Spend`
*   **TACoS**: Percentage of total revenue spent on ads.
    *   *Calculation*: `(Ad Spend ÷ Total Revenue) × 100`

### 📈 Charts & Analysis
*   **Sales Trend**: Daily revenue vs. the previous period. Helps you see if current sales are "Above" or "Below" normal.
*   **Platform Split**: Shows which sales channel (Amazon, Flipkart, etc.) is contributing the most revenue.
*   **Category Performance**: A table showing which product categories are growing the fastest and which are declining.
*   **Critical Alerts**: Automated flags for issues like "Sudden Sales Drop" or "High Ad Spend."
*   **Revenue Forecast**: Predicated month-end revenue based on your current daily pace.

---

## 📈 2. Business Dashboard
**Target Audience**: Business Heads & Department Leads
**Focus**: Channel Performance, Marketing ROI, and Operational Efficiency.

### 📊 KPI Cards (Key Metrics)
*   **Gross Margin**: Profitability of your products before marketing costs.
    *   *Calculation*: `(Revenue - Product Cost) ÷ Revenue × 100`
*   **Ad Spend**: Total investment in advertising across all platforms.
    *   *Calculation*: Sum of SP, SB, and SD campaign costs.
*   **Orders & Units**: Volume of physical sales.
    *   *Calculation*: Count of unique orders and total items moved.
*   **AOV (Average Order Value)**: The typical spend per customer order.
    *   *Calculation*: `Total Revenue ÷ Total Orders`

### 📈 Charts & Analysis
*   **Marketing Performance**: Breakdown of **ROAS** (return on ad rupee) and **TACoS**.
    *   *ROAS Calculation*: `Revenue ÷ Ad Spend`
*   **Inventory Overview**: Quick view of how many products are **In Stock**, **Low Stock**, or **Out of Stock**.
*   **OOS Impact**: The revenue you are *missing out on* because products are out of stock.
    *   *Calculation*: `Average Daily Sales × Days Out of Stock`
*   **Profitability Overview**: Tracking your margins over time to ensure the business remains sustainable.

---

## 🏷️ 3. Category Dashboard
**Target Audience**: Category Managers & SKU Specialists
**Focus**: Product-level deep dives, Cluster performance, and Stocking.

### 📊 KPI Cards (Key Metrics)
*   **Return Rate**: Percentage of items being sent back by customers.
    *   *Calculation*: `(Returned Units ÷ Total Units Sold) × 100`
*   **Active SKUs**: Count of products that have had at least one sale.
*   **Category Health**: A summary of stock levels for the specific category you have filtered.

### 📈 Charts & Analysis
*   **Cluster Performance**: Breakdown of sales by sub-groups (e.g., in a "Kitchen" category, clusters might be "Tools" vs "Storage").
*   **Top Performing Products**: Table showing your "Winners" by growth and revenue.
*   **Underperforming Products**: Highlights items with falling sales that may need pricing or ad adjustments.
*   **Inventory Position (Days of Cover)**: Tells you how many days your current stock will last.
    *   *Calculation*: `Current Stock ÷ Average Daily Sales (with a buffer for traffic)`
*   **Growth Opportunities**: Identifying search terms or products with high traffic but low sales, where you can easily improve performance.

---

## ⚙️ General Tips for All Users

### Using Filters
*   **Date Filter**: You can view data for "Today," "Yesterday," "Last 7 Days," or custom ranges.
*   **Category/SKU Filter**: Use these to drill down into a specific area of the business.

### Color Coding
*   🟢 **Green**: Metric is improving or stock is healthy (over 30 days).
*   🟡 **Amber**: Metric is stable or stock is low (7–30 days).
*   🔴 **Red**: Metric is declining significantly or product is out of stock (under 7 days).

---

*Need the full technical logic? See the [Calculations & Logic Guide](file:///Users/apple/Documents/Devarth%20Nanavaty/Backup/Python/Django/Plantex%20Sales%20Dashboard/dashboards_logic.md).*
