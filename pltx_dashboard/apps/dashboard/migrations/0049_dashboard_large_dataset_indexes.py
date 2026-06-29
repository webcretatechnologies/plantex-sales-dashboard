from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0048_dashboardproductdailysummary_spend_sb_and_more"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="categorymapping",
            index=models.Index(fields=["user", "launch_date", "asin"], name="idx_cm_u_launch_asin"),
        ),
        migrations.AddIndex(
            model_name="categorymapping",
            index=models.Index(fields=["user", "category", "launch_date"], name="idx_cm_u_cat_launch"),
        ),
        migrations.AddIndex(
            model_name="processeddashboarddata",
            index=models.Index(fields=["user", "category", "date", "asin"], name="idx_pdd_u_cat_d_asn"),
        ),
        migrations.AddIndex(
            model_name="processeddashboarddata",
            index=models.Index(fields=["user", "portfolio", "date", "asin"], name="idx_pdd_u_port_d_asn"),
        ),
        migrations.AddIndex(
            model_name="flipkartcategorymap",
            index=models.Index(fields=["user", "asin"], name="idx_fkcat_u_asin"),
        ),
        migrations.AddIndex(
            model_name="flipkartcategorymap",
            index=models.Index(fields=["user", "launch_date", "fsn"], name="idx_fkcat_u_launch_fsn"),
        ),
        migrations.AddIndex(
            model_name="flipkartcategorymap",
            index=models.Index(fields=["user", "category", "launch_date"], name="idx_fkcat_u_cat_launch"),
        ),
        migrations.AddIndex(
            model_name="flipkartprocesseddashboarddata",
            index=models.Index(fields=["user", "category", "date", "fsn"], name="idx_fkpd_u_cat_d_fsn"),
        ),
        migrations.AddIndex(
            model_name="flipkartprocesseddashboarddata",
            index=models.Index(fields=["user", "portfolio", "date", "fsn"], name="idx_fkpd_u_port_d_fsn"),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(fields=["user", "platform", "asin", "date"], name="idx_dpds_u_p_asn_d"),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(fields=["user", "platform", "fsn", "date"], name="idx_dpds_u_p_fsn_d"),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(fields=["user", "platform", "category", "date", "asin"], name="idx_dpds_u_p_c_d_a"),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(fields=["user", "platform", "category", "date", "fsn"], name="idx_dpds_u_p_c_d_f"),
        ),
        migrations.AddIndex(
            model_name="dashboardinventoryhealthsummary",
            index=models.Index(fields=["user", "platform", "date", "status_class"], name="idx_dihs_u_p_d_sc"),
        ),
        migrations.AddIndex(
            model_name="dashboardinventoryhealthsummary",
            index=models.Index(fields=["user", "platform", "date", "fk_status_class"], name="idx_dihs_u_p_d_fsc"),
        ),
        migrations.AddIndex(
            model_name="dashboardinventoryhealthsummary",
            index=models.Index(fields=["user", "platform", "category", "date", "status_class"], name="idx_dihs_u_p_c_d_sc"),
        ),
    ]
