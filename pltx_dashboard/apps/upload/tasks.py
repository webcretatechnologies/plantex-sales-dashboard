from celery import shared_task
from .models import UploadedFile
from apps.accounts.models import Users
from .services import (
    process_category_file, process_price_file, process_spend_file, process_sales_file,
    process_flipkart_sales_file, process_flipkart_inventory_file,
    process_flipkart_pca_file, process_flipkart_pla_file,
    generate_dashboard_data
)
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

@shared_task
def process_file_task(uploaded_file_id, file_type, user_id, date_str, is_last):
    try:
        user = Users.objects.get(id=user_id)
        uploaded = UploadedFile.objects.get(id=uploaded_file_id)
        
        channel_layer = get_channel_layer()
        group_name = f'user_{user_id}'

        async_to_sync(channel_layer.group_send)(
            group_name, {
                'type': 'upload_progress',
                'message': f'Processing {file_type} file...',
                'status': 'processing'
            }
        )

        if file_type == 'category':
            process_category_file(uploaded.file.path, user)
        elif file_type == 'price':
            process_price_file(uploaded.file.path, user)
        elif file_type == 'spend':
            process_spend_file(uploaded.file.path, user)
        elif file_type == 'sales':
            process_sales_file(uploaded.file.path, date_str, user)
        elif file_type == 'flipkart_sales':
            process_flipkart_sales_file(uploaded.file.path, user)
        elif file_type == 'flipkart_inventory':
            process_flipkart_inventory_file(uploaded.file.path, user)
        elif file_type == 'flipkart_pca':
            process_flipkart_pca_file(uploaded.file.path, user)
        elif file_type == 'flipkart_pla':
            process_flipkart_pla_file(uploaded.file.path, user)
        
        if is_last:
            async_to_sync(channel_layer.group_send)(
                group_name, {
                    'type': 'upload_progress',
                    'message': 'Generating final dashboard data...',
                    'status': 'processing'
                }
            )
            generate_dashboard_data(user)
            async_to_sync(channel_layer.group_send)(
                group_name, {
                    'type': 'upload_progress',
                    'message': 'All files processed successfully!',
                    'status': 'complete'
                }
            )
        else:
            async_to_sync(channel_layer.group_send)(
                group_name, {
                    'type': 'upload_progress',
                    'message': f'{file_type} processed successfully.',
                    'status': 'partial'
                }
            )

    except Exception as e:
        if 'group_name' in locals():
            async_to_sync(channel_layer.group_send)(
                group_name, {
                    'type': 'upload_progress',
                    'message': f'Error processing file: {str(e)}',
                    'status': 'error'
                }
            )

