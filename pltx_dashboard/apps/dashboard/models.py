from django.db import models


class SalesData(models.Model):
    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="sales_records"
    )
    date = models.DateField(db_index=True)
    asin = models.CharField(max_length=50, db_index=True)
    pageviews = models.IntegerField(default=0)
    units = models.IntegerField(default=0)
    orders = models.IntegerField(default=0)
    revenue = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "date", "asin")


class SpendData(models.Model):
    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="spend_records"
    )
    date = models.DateField(db_index=True)
    asin = models.CharField(max_length=50, db_index=True)
    ad_account = models.CharField(max_length=100)
    ad_type = models.CharField(max_length=10)
    spend = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "date", "asin", "ad_account", "ad_type")


class CategoryMapping(models.Model):
    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="category_mappings"
    )
    asin = models.CharField(max_length=50, db_index=True)
    portfolio = models.CharField(max_length=100)
    category = models.CharField(max_length=100)
    subcategory = models.CharField(max_length=100)

    class Meta:
        unique_together = ("user", "asin")


class PriceData(models.Model):
    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="price_data"
    )
    asin = models.CharField(max_length=50, db_index=True)
    price = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "asin")


class FBAStockData(models.Model):
    """FBA Stock file — per-ASIN ending warehouse balance."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fba_stock_data"
    )
    date = models.DateField(db_index=True, null=True, blank=True)
    fnsku = models.CharField(max_length=50, null=True, blank=True)
    asin = models.CharField(max_length=50, db_index=True)
    msku = models.CharField(max_length=100, null=True, blank=True)
    title = models.CharField(max_length=500, null=True, blank=True)
    disposition = models.CharField(max_length=50, null=True, blank=True)
    starting_warehouse_balance = models.IntegerField(default=0)
    in_transit_between_warehouses = models.IntegerField(default=0)
    receipts = models.IntegerField(default=0)
    customer_shipments = models.IntegerField(default=0)
    customer_returns = models.IntegerField(default=0)
    vendor_returns = models.IntegerField(default=0)
    warehouse_transfer_in_out = models.IntegerField(default=0)
    found = models.IntegerField(default=0)
    lost = models.IntegerField(default=0)
    damaged = models.IntegerField(default=0)
    disposed = models.IntegerField(default=0)
    other_events = models.IntegerField(default=0)
    ending_warehouse_balance = models.IntegerField(default=0)
    unknown_events = models.IntegerField(default=0)
    location = models.CharField(max_length=200, null=True, blank=True)

    class Meta:
        unique_together = ("user", "asin", "date", "disposition", "location")
        indexes = [
            models.Index(fields=["user", "asin"], name="idx_fba_user_asin"),
        ]

    def __str__(self):
        return f"FBA Stock: {self.asin} ({self.ending_warehouse_balance})"


class FlexStockData(models.Model):
    """Flex Stock file — per-ASIN cluster-level stock quantity."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="flex_stock_data"
    )
    date = models.DateField(db_index=True, null=True, blank=True)
    asin = models.CharField(max_length=50, db_index=True)
    cluster = models.CharField(max_length=100, null=True, blank=True)
    qty = models.IntegerField(default=0)

    class Meta:
        unique_together = ("user", "asin", "date", "cluster")
        indexes = [
            models.Index(fields=["user", "asin"], name="idx_flex_user_asin"),
            models.Index(fields=["user", "asin", "date"], name="idx_flex_u_a_d"),
        ]

    def __str__(self):
        return f"Flex Stock: {self.asin} ({self.qty}) [{self.date}]"


class ProcessedDashboardData(models.Model):
    user = models.ForeignKey(
        "accounts.Users",
        on_delete=models.CASCADE,
        related_name="processed_dashboard_data",
    )
    date = models.DateField(db_index=True)
    asin = models.CharField(max_length=50, db_index=True)
    portfolio = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    category = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    subcategory = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    price = models.FloatField(default=0.0)

    pageviews = models.IntegerField(default=0)
    units = models.IntegerField(default=0)
    orders = models.IntegerField(default=0)
    revenue = models.FloatField(default=0.0)

    spend_sp = models.FloatField(default=0.0)
    spend_sb = models.FloatField(default=0.0)
    spend_sd = models.FloatField(default=0.0)
    total_spend = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "date", "asin")
        indexes = [
            models.Index(fields=["user", "category", "date"], name="idx_user_cat_date"),
            models.Index(fields=["user", "date"], name="idx_user_date"),
            models.Index(fields=["user", "asin", "date"], name="idx_user_asin_date"),
            models.Index(
                fields=["user", "date", "portfolio", "category"],
                name="idx_u_d_p_c",
            ),
        ]


# ============================================================================
# SLIM FLIPKART MODELS (dashboard pipeline — only required columns)
# ============================================================================


class FlipkartSearchTraffic(models.Model):
    """Search Traffic Report — FSN-level traffic & sales per date."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_search_traffic"
    )
    fsn = models.CharField(max_length=50, db_index=True)
    sku = models.CharField(max_length=100, null=True, blank=True)
    vertical = models.CharField(max_length=100, null=True, blank=True)
    date = models.DateField(db_index=True)
    page_views = models.IntegerField(default=0)
    product_clicks = models.IntegerField(default=0)
    sales = models.IntegerField(default=0)
    revenue = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "fsn", "date")


class FlipkartCategoryMap(models.Model):
    """Category Dashboard — FSN → Portfolio / Category / SubCategory."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_category_maps"
    )
    fsn = models.CharField(max_length=50, db_index=True)
    sku = models.CharField(max_length=100, null=True, blank=True)
    portfolio = models.CharField(max_length=100, null=True, blank=True)
    category = models.CharField(max_length=100, null=True, blank=True)
    subcategory = models.CharField(max_length=100, null=True, blank=True)
    product_status = models.CharField(max_length=30, null=True, blank=True, db_index=True)

    class Meta:
        unique_together = ("user", "fsn")


class FlipkartPrice(models.Model):
    """FK Price — FSN → Deal price."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_price_data"
    )
    fsn = models.CharField(max_length=50, db_index=True)
    price = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "fsn")


class FlipkartPLA(models.Model):
    """PLA FSN Report — campaign_id + FSN → Ad Spend."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_pla_reports"
    )
    campaign_id = models.CharField(max_length=100, db_index=True)
    fsn_id = models.CharField(max_length=100, db_index=True)
    date = models.DateField(db_index=True, null=True, blank=True)
    ad_spend = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "campaign_id", "fsn_id", "date")


class FlipkartInventoryStock(models.Model):
    """FK Inventory file — FSN-level current stock snapshot (FK.xlsx)."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_inventory_stock"
    )
    fsn = models.CharField(max_length=50, db_index=True)
    sku = models.CharField(max_length=200, null=True, blank=True)
    product_status = models.CharField(max_length=50, null=True, blank=True)
    product_type = models.CharField(max_length=200, null=True, blank=True)
    qty = models.IntegerField(default=0)

    class Meta:
        unique_together = ("user", "fsn")
        verbose_name = "Flipkart Inventory Stock"

    def __str__(self):
        return f"FK Inventory: {self.fsn} ({self.qty})"


class FlipkartProcessedDashboardData(models.Model):
    """Final merged Flipkart data — analogous to ProcessedDashboardData."""

    user = models.ForeignKey(
        "accounts.Users",
        on_delete=models.CASCADE,
        related_name="fk_processed_dashboard",
    )
    date = models.DateField(db_index=True)
    fsn = models.CharField(max_length=50, db_index=True)
    platform = models.CharField(max_length=20, default="Flipkart")
    portfolio = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    category = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    subcategory = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    price = models.FloatField(default=0.0)

    pageviews = models.IntegerField(default=0)
    units = models.IntegerField(default=0)
    orders = models.IntegerField(default=0)  # always 0 for Flipkart (no order data)
    revenue = models.FloatField(default=0.0)

    total_spend = models.FloatField(default=0.0)
    spend_sp = models.FloatField(
        default=0.0
    )  # not split for Flipkart; all in total_spend
    spend_sb = models.FloatField(default=0.0)
    spend_sd = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "date", "fsn")
        indexes = [
            models.Index(
                fields=["user", "category", "date"], name="idx_fk_user_cat_date"
            ),
            models.Index(fields=["user", "date"], name="idx_fk_user_date"),
            models.Index(fields=["user", "fsn", "date"], name="idx_fk_user_fsn_date"),
            models.Index(
                fields=["user", "date", "portfolio", "category"],
                name="idx_fk_u_d_p_c",
            ),
        ]
