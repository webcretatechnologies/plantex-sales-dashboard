from django.db import models
from django.core.files.storage import FileSystemStorage
import os

class OverwriteStorage(FileSystemStorage):
    def get_available_name(self, name, max_length=None):
        if self.exists(name):
            os.remove(os.path.join(self.location, name))
        return name

def user_directory_path(instance, filename):
    return f'uploads/{instance.user.id}/{filename}'

class UploadedFile(models.Model):
    user = models.ForeignKey('accounts.Users', on_delete=models.CASCADE, related_name='uploaded_files')
    file = models.FileField(upload_to=user_directory_path, storage=OverwriteStorage())
    file_type = models.CharField(max_length=50)  # sales, spend, category, price
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.file_type} - {self.file.name}"
