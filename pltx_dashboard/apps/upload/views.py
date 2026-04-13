from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from .models import UploadedFile
from apps.accounts.models import Users
import os

class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)
    authentication_classes = []

    def post(self, request, *args, **kwargs):
        user_id = request.session.get('user_id')
        if not user_id:
            return Response({'error': 'Not authenticated'}, status=401)

        try:
            user = Users.objects.get(id=user_id)
        except Users.DoesNotExist:
            return Response({'error': 'User not found'}, status=401)

        # RBAC Check
        if not user.is_main_user:
            if not user.role or not user.role.features.filter(code_name='upload_data').exists():
                return Response({'error': 'Permission Denied'}, status=403)


        file_obj = request.FILES.get('file')
        file_type = request.data.get('file_type') # 'sales', 'spend', 'category', 'price'
        date_str = request.data.get('date', '') # needed for sales optionally if not in filename

        if not file_obj or not file_type:
            return Response({'error': 'file and file_type are required'}, status=400)

        expected_path = f"uploads/{user.id}/{os.path.basename(file_obj.name)}"
        UploadedFile.objects.filter(user=user, file=expected_path).delete()

        uploaded = UploadedFile.objects.create(file=file_obj, file_type=file_type, user=user)

        try:
            is_last = request.data.get('is_last') == 'true'
            filename = os.path.basename(file_obj.name)
            if file_type == 'sales' and not date_str:
                date_str = os.path.splitext(filename)[0][:10]

            from .tasks import process_file_task
            process_file_task.delay(uploaded.id, file_type, user.id, date_str, is_last)

            return Response({'message': 'File uploading... Processing in background.'}, status=200)

        except Exception as e:
            return Response({'error': str(e)}, status=500)
