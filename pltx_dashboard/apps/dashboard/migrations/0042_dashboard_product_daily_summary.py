from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0041_repair_launch_date_columns"),
    ]

    operations = [
        migrations.CreateModel(
            name="DashboardProductDailySummary",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("date", models.DateField(db_index=True)),
                ("platform", models.CharField(db_index=True, max_length=20)),
                ("sku", models.CharField(db_index=True, max_length=80)),
                (
                    "asin",
                    models.CharField(
                        blank=True, db_index=True, max_length=50, null=True
                    ),
                ),
                (
                    "fsn",
                    models.CharField(
                        blank=True, db_index=True, max_length=80, null=True
                    ),
                ),
                (
                    "portfolio",
                    models.CharField(
                        blank=True, db_index=True, max_length=100, null=True
                    ),
                ),
                (
                    "category",
                    models.CharField(
                        blank=True, db_index=True, max_length=100, null=True
                    ),
                ),
                (
                    "subcategory",
                    models.CharField(
                        blank=True, db_index=True, max_length=100, null=True
                    ),
                ),
                ("revenue", models.FloatField(default=0.0)),
                ("units_sold", models.IntegerField(default=0)),
                ("page_views", models.IntegerField(default=0)),
                ("orders", models.IntegerField(default=0)),
                ("ad_spend", models.FloatField(default=0.0)),
                ("product_clicks", models.IntegerField(default=0)),
                ("sales", models.IntegerField(default=0)),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="dashboard_product_daily_summaries",
                        to="accounts.users",
                    ),
                ),
            ],
            options={
                "unique_together": {("user", "date", "platform", "sku")},
            },
        ),
        migrations.AddIndex(
            model_name="salesdata",
            index=models.Index(
                fields=["user", "asin", "date"], name="idx_sales_u_asin_d"
            ),
        ),
        migrations.AddIndex(
            model_name="spenddata",
            index=models.Index(
                fields=["user", "asin", "date"], name="idx_spend_u_asin_d"
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "date", "platform"], name="idx_dpds_u_d_p"
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "platform", "sku", "date"],
                name="idx_dpds_u_p_s_d",
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(fields=["user", "asin", "date"], name="idx_dpds_u_asn_d"),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(fields=["user", "fsn", "date"], name="idx_dpds_u_fsn_d"),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "category", "date"], name="idx_dpds_u_cat_d"
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "portfolio", "date"], name="idx_dpds_u_port_d"
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "subcategory", "date"], name="idx_dpds_u_sub_d"
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "platform", "category", "date"],
                name="idx_dpds_u_p_cat_d",
            ),
        ),
    ]
