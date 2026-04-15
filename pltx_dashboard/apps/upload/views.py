from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
import os

from apps.accounts.utils import get_logged_in_user
from .services import (
    process_category_file, process_price_file, process_spend_file, process_sales_file,
    generate_dashboard_data,
    # Slim Flipkart pipeline
    process_fk_search_traffic, process_fk_category, process_fk_price,
    process_fk_pca, process_fk_pla, process_fk_sales_invoice, process_fk_coupon,
    generate_flipkart_dashboard_data,
)

class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)
    authentication_classes = []

    def post(self, request, *args, **kwargs):
        user = get_logged_in_user(request)
        if not user:
            return Response({'error': 'Not authenticated'}, status=401)

        # RBAC Check
        if not user.is_main_user:
            if not user.role or not user.role.features.filter(code_name='upload_data').exists():
                return Response({'error': 'Permission Denied'}, status=403)

        # Use data_owner (the main user) for data associations to avoid duplicates
        data_owner = user.created_by if user.created_by else user

        file_obj = request.FILES.get('file')
        file_type = request.data.get('file_type') # 'sales', 'spend', 'category', etc.
        date_str = request.data.get('date', '') 

        if not file_obj or not file_type:
            return Response({'error': 'file and file_type are required'}, status=400)

        is_last = request.data.get('is_last') == 'true'
        filename = os.path.basename(file_obj.name)
        
        if file_type == 'sales' and not date_str:
            date_str = os.path.splitext(filename)[0][:10]

        channel_layer = get_channel_layer()
        group_name = f'user_{user.id}'

        # Send starting message
        async_to_sync(channel_layer.group_send)(
            group_name, {
                'type': 'upload_progress',
                'message': f'Processing {file_type} file...',
                'status': 'processing'
            }
        )

        try:
            # Process directly from memory (file_obj) without saving to disk
            if file_type == 'category':
                process_category_file(file_obj, data_owner)
            elif file_type == 'price':
                process_price_file(file_obj, data_owner)
            elif file_type == 'spend':
                process_spend_file(file_obj, data_owner)
            elif file_type == 'sales':
                process_sales_file(file_obj, date_str, data_owner)
            # Slim Flipkart pipeline
            elif file_type == 'fk_search_traffic':
                process_fk_search_traffic(file_obj, data_owner)
            elif file_type == 'fk_category':
                process_fk_category(file_obj, data_owner)
            elif file_type == 'fk_price':
                process_fk_price(file_obj, data_owner)
            elif file_type == 'fk_pca':
                process_fk_pca(file_obj, data_owner)
            elif file_type == 'fk_pla':
                process_fk_pla(file_obj, data_owner)
            elif file_type == 'fk_sales_invoice':
                process_fk_sales_invoice(file_obj, data_owner)
            elif file_type == 'fk_coupon':
                process_fk_coupon(file_obj, data_owner)

            # Determine which pipeline to run on the last file
            fk_types = {'fk_search_traffic', 'fk_category', 'fk_price',
                        'fk_pca', 'fk_pla', 'fk_sales_invoice', 'fk_coupon'}
            is_flipkart = file_type in fk_types

            if is_last:
                async_to_sync(channel_layer.group_send)(
                    group_name, {
                        'type': 'upload_progress',
                        'message': 'Generating final dashboard data...',
                        'status': 'processing'
                    }
                )
                if is_flipkart:
                    generate_flipkart_dashboard_data(data_owner)
                else:
                    generate_dashboard_data(data_owner)
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

            return Response({'message': 'File processed successfully'}, status=200)

        except Exception as e:
            async_to_sync(channel_layer.group_send)(
                group_name, {
                    'type': 'upload_progress',
                    'message': f'Error processing file: {str(e)}',
                    'status': 'error'
                }
            )
            return Response({'error': str(e)}, status=500)
