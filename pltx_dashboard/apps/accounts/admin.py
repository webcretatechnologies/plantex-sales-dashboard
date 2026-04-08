from django.contrib import admin
from .models import Users


@admin.register(Users)
class UsersAdmin(admin.ModelAdmin):
    list_display = ('fname', 'lname', 'email')
    search_fields = ('fname', 'lname', 'email')
