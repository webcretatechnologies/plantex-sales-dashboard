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


class FlipkartPCA(models.Model):
    """PCA Attribution — maps FSN → campaign_id (for spend join)."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_pca_reports"
    )
    campaign_id = models.CharField(max_length=100, db_index=True)
    campaign_name = models.CharField(max_length=255, null=True, blank=True)
    date = models.DateField(db_index=True, null=True, blank=True)
    fsn_id = models.CharField(max_length=100, db_index=True)

    class Meta:
        unique_together = ("user", "campaign_id", "fsn_id", "date")


class FlipkartPLA(models.Model):
    """PLA FSN Report — campaign_id + FSN → Ad Spend."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_pla_reports"
    )
    campaign_id = models.CharField(max_length=100, db_index=True)
    fsn_id = models.CharField(max_length=100, db_index=True)
    ad_spend = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "campaign_id", "fsn_id")


class FlipkartSalesInvoice(models.Model):
    """Cash Back Report sheet — taxable value & invoice amount per order item."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_sales_invoices"
    )
    order_id = models.CharField(max_length=100, db_index=True)
    order_item_id = models.CharField(max_length=100, db_index=True)
    fsn = models.CharField(max_length=100, db_index=True, null=True, blank=True)
    item_quantity = models.IntegerField(default=0)
    taxable_value = models.FloatField(default=0.0)
    invoice_amount = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "order_id", "order_item_id")


class FlipkartCoupon(models.Model):
    """Coupon Value Report — coupon value per FSN."""

    user = models.ForeignKey(
        "accounts.Users", on_delete=models.CASCADE, related_name="fk_coupon_data"
    )
    fsn = models.CharField(max_length=50, db_index=True)
    coupon_value = models.FloatField(default=0.0)

    class Meta:
        unique_together = ("user", "fsn")


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

    taxable_value = models.FloatField(default=0.0)
    invoice_amount = models.FloatField(default=0.0)
    coupon_total = models.FloatField(default=0.0)
    coupon_error = models.BooleanField(default=False)

    class Meta:
        unique_together = ("user", "date", "fsn")
        indexes = [
            models.Index(
                fields=["user", "category", "date"], name="idx_fk_user_cat_date"
            ),
            models.Index(fields=["user", "date"], name="idx_fk_user_date"),
            models.Index(fields=["user", "fsn", "date"], name="idx_fk_user_fsn_date"),
        ]
