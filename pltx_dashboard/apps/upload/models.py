from django.db import models
from django.core.files.storage import FileSystemStorage
import os

# These are kept ONLY for historical migration compatibility
class OverwriteStorage(FileSystemStorage):
    def get_available_name(self, name, max_length=None):
        if self.exists(name):
            os.remove(os.path.join(self.location, name))
        return name

def user_directory_path(instance, filename):
    return f'uploads/{instance.user.id}/{filename}'

# UploadedFile model has been removed. Uploads are processed synchronously in-memory.
