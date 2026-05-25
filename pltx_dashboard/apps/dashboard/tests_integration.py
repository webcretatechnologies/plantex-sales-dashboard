from datetime import date

from django.test import TestCase, override_settings
from django.urls import reverse

from apps.accounts.models import Feature, Role, Users
from apps.dashboard.models import (
    CategoryMapping,
    FlipkartProcessedDashboardData,
    ProcessedDashboardData,
)
from apps.dashboard.services.analytics_services_orm_pipeline import run_orm_computation
from apps.dashboard.services.daily_summary import rebuild_daily_summary_for_user


@override_settings(
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "dashboard-tests",
        }
    }
)
class DashboardEndpointIntegrationTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.feature_business = Feature.objects.create(
            name="Business Dashboard", code_name="business_dashboard"
        )
        cls.feature_upload = Feature.objects.create(
            name="Upload Data", code_name="upload_data"
        )

        cls.main_user = Users.objects.create(
            fname="Main",
            lname="Owner",
            email="main-owner@example.com",
            pswd="secret",
            cpswd="secret",
        )

        dashboard_role = Role.objects.create(
            name="Dashboard Role", created_by=cls.main_user
        )
        dashboard_role.features.add(cls.feature_business)

        upload_only_role = Role.objects.create(
            name="Upload Only Role", created_by=cls.main_user
        )
        upload_only_role.features.add(cls.feature_upload)

        cls.allowed_sub_user = Users.objects.create(
            fname="Allowed",
            lname="User",
            email="allowed-sub@example.com",
            pswd="secret",
            cpswd="secret",
            created_by=cls.main_user,
            role=dashboard_role,
        )
        cls.denied_sub_user = Users.objects.create(
            fname="Denied",
            lname="User",
            email="denied-sub@example.com",
            pswd="secret",
            cpswd="secret",
            created_by=cls.main_user,
            role=upload_only_role,
        )

        rows = []
        base_date = date(2026, 5, 1)
        for i in range(1, 261):
            rows.append(
                ProcessedDashboardData(
                    user=cls.main_user,
                    date=base_date,
                    asin=f"ASIN{i:04d}",
                    portfolio=f"Portfolio {i % 6}",
                    category=f"Category {i % 8}",
                    subcategory=f"Subcategory {i % 10}",
                    price=499.0 + i,
                    pageviews=100 + i,
                    units=max(1, i % 11),
                    orders=max(1, i % 9),
                    revenue=1000.0 + i * 5,
                    spend_sp=80.0 + (i % 5),
                    spend_sb=20.0 + (i % 3),
                    spend_sd=10.0 + (i % 2),
                    total_spend=110.0 + (i % 7),
                )
            )
        ProcessedDashboardData.objects.bulk_create(rows, batch_size=500)

    def _login(self, user):
        session = self.client.session
        session["user_id"] = user.id
        session.save()

    def test_dashboard_section_denies_sub_user_without_dashboard_feature(self):
        self._login(self.denied_sub_user)
        response = self.client.get(
            reverse(
                "dashboard-section",
                kwargs={"view_name": "business", "section": "overview"},
            )
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json().get("error"), "Permission denied.")

    def test_dashboard_section_allows_sub_user_with_feature(self):
        self._login(self.allowed_sub_user)
        response = self.client.get(
            reverse(
                "dashboard-section",
                kwargs={"view_name": "business", "section": "overview"},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Total Revenue")

    def test_filter_dropdown_denies_sub_user_without_dashboard_feature(self):
        self._login(self.denied_sub_user)
        response = self.client.get(
            reverse("dashboard-filter-options"),
            {"field": "asin", "page": 1, "page_size": 50},
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json().get("error"), "Permission denied.")

    def test_filter_dropdown_paginates_large_dataset(self):
        self._login(self.main_user)
        response = self.client.get(
            reverse("dashboard-filter-options"),
            {"field": "asin", "page": 1, "page_size": 50},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["field"], "asin")
        self.assertEqual(len(payload["results"]), 50)
        self.assertEqual(payload["pagination"]["total"], 260)
        self.assertTrue(payload["pagination"]["has_next"])

        response_last_page = self.client.get(
            reverse("dashboard-filter-options"),
            {"field": "asin", "page": 6, "page_size": 50},
        )
        self.assertEqual(response_last_page.status_code, 200)
        payload_last = response_last_page.json()
        self.assertEqual(len(payload_last["results"]), 10)
        self.assertFalse(payload_last["pagination"]["has_next"])

    def test_filter_dropdown_search_is_case_insensitive(self):
        self._login(self.main_user)
        response = self.client.get(
            reverse("dashboard-filter-options"),
            {"field": "asin", "q": "asin02", "page": 1, "page_size": 100},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertGreater(len(payload["results"]), 0)
        for row in payload["results"]:
            self.assertIn("ASIN02", row["value"])


@override_settings(
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "dashboard-summary-tests",
        }
    }
)
class DashboardSummaryFastPathTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = Users.objects.create(
            fname="Summary",
            lname="Owner",
            email="summary-owner@example.com",
            pswd="secret",
            cpswd="secret",
        )

        CategoryMapping.objects.bulk_create(
            [
                CategoryMapping(
                    user=cls.user,
                    asin="ASIN-ALPHA",
                    portfolio="Planters",
                    category="Indoor",
                    subcategory="Ceramic",
                ),
                CategoryMapping(
                    user=cls.user,
                    asin="ASIN-BETA",
                    portfolio="Planters",
                    category="Outdoor",
                    subcategory="Metal",
                ),
            ]
        )

        rows = [
            ProcessedDashboardData(
                user=cls.user,
                date=date(2026, 1, 10),
                asin="ASIN-ALPHA",
                portfolio="Planters",
                category="Indoor",
                subcategory="Ceramic",
                price=499.0,
                pageviews=120,
                units=8,
                orders=7,
                revenue=8000.0,
                spend_sp=600.0,
                spend_sb=100.0,
                spend_sd=50.0,
                total_spend=750.0,
            ),
            ProcessedDashboardData(
                user=cls.user,
                date=date(2026, 1, 11),
                asin="ASIN-BETA",
                portfolio="Planters",
                category="Outdoor",
                subcategory="Metal",
                price=699.0,
                pageviews=80,
                units=5,
                orders=5,
                revenue=5000.0,
                spend_sp=300.0,
                spend_sb=60.0,
                spend_sd=40.0,
                total_spend=400.0,
            ),
            ProcessedDashboardData(
                user=cls.user,
                date=date(2026, 2, 10),
                asin="ASIN-ALPHA",
                portfolio="Planters",
                category="Indoor",
                subcategory="Ceramic",
                price=499.0,
                pageviews=180,
                units=12,
                orders=10,
                revenue=12000.0,
                spend_sp=700.0,
                spend_sb=120.0,
                spend_sd=80.0,
                total_spend=900.0,
            ),
            ProcessedDashboardData(
                user=cls.user,
                date=date(2026, 2, 11),
                asin="ASIN-BETA",
                portfolio="Planters",
                category="Outdoor",
                subcategory="Metal",
                price=699.0,
                pageviews=60,
                units=0,
                orders=0,
                revenue=0.0,
                spend_sp=0.0,
                spend_sb=0.0,
                spend_sd=0.0,
                total_spend=0.0,
            ),
        ]
        ProcessedDashboardData.objects.bulk_create(rows)
        FlipkartProcessedDashboardData.objects.bulk_create(
            [
                FlipkartProcessedDashboardData(
                    user=cls.user,
                    date=date(2026, 2, 10),
                    fsn="FSN-ALPHA",
                    portfolio="Planters",
                    category="Indoor",
                    subcategory="Ceramic",
                    price=549.0,
                    pageviews=40,
                    units=3,
                    orders=0,
                    revenue=2100.0,
                    total_spend=150.0,
                    spend_sp=150.0,
                    spend_sb=0.0,
                    spend_sd=0.0,
                ),
                FlipkartProcessedDashboardData(
                    user=cls.user,
                    date=date(2026, 2, 11),
                    fsn="FSN-BETA",
                    portfolio="Planters",
                    category="Outdoor",
                    subcategory="Metal",
                    price=799.0,
                    pageviews=25,
                    units=1,
                    orders=0,
                    revenue=900.0,
                    total_spend=50.0,
                    spend_sp=50.0,
                    spend_sb=0.0,
                    spend_sd=0.0,
                ),
            ]
        )
        rebuild_daily_summary_for_user(cls.user)

    def test_summary_backed_payload_matches_raw_payload_for_core_analytics(self):
        filters = {
            "start_date": "2026-02-10",
            "end_date": "2026-02-11",
        }
        qs = ProcessedDashboardData.objects.filter(user=self.user)
        fk_qs = FlipkartProcessedDashboardData.objects.filter(user=self.user)

        summary_payload = run_orm_computation(
            qs,
            fk_qs,
            None,
            filters,
            self.user,
            cached_filter_metadata={"platforms": ["Amazon"], "dates": []},
            include_full_payload=False,
        )
        raw_payload = run_orm_computation(
            qs,
            fk_qs,
            None,
            filters,
            self.user,
            cached_filter_metadata={"platforms": ["Amazon"], "dates": []},
            include_full_payload=True,
        )

        for key in (
            "revenue",
            "orders",
            "units",
            "pageviews",
            "spend",
            "roas",
            "tacos",
            "active_asins",
            "revenue_change",
            "mom_growth",
            "yoy_growth",
        ):
            self.assertAlmostEqual(
                summary_payload["kpis"][key],
                raw_payload["kpis"][key],
                places=2,
            )

        self.assertEqual(summary_payload["platforms"], raw_payload["platforms"])
        self.assertEqual(
            summary_payload["category_performance"],
            raw_payload["category_performance"],
        )
        self.assertEqual(
            summary_payload["cluster_performance"],
            raw_payload["cluster_performance"],
        )
        self.assertEqual(summary_payload["cat_top_products"], [])
        self.assertEqual(summary_payload["cat_under_products"], [])

    def test_summary_backed_charts_include_platform_trend_without_field_error(self):
        filters = {
            "start_date": "2026-02-10",
            "end_date": "2026-02-11",
        }
        payload = run_orm_computation(
            ProcessedDashboardData.objects.filter(user=self.user),
            FlipkartProcessedDashboardData.objects.filter(user=self.user),
            None,
            filters,
            self.user,
            cached_filter_metadata={"platforms": ["Amazon", "Flipkart"], "dates": []},
            include_full_payload=False,
        )

        trend = payload["charts"]["trend"]
        self.assertEqual(trend["labels"], ["2026-02-10", "2026-02-11"])
        self.assertEqual(trend["amazon_revenue"], [12000.0, 0.0])
        self.assertEqual(trend["flipkart_revenue"], [2100.0, 900.0])
