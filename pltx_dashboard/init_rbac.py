import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pltx_dashboard.settings')
django.setup()

from apps.accounts.models import Feature, Users, Role

def init_rbac():
    features_data = [
        {'name': 'CEO Dashboard', 'code_name': 'ceo_dashboard'},
        {'name': 'Business Dashboard', 'code_name': 'business_dashboard'},
        {'name': 'Category Dashboard', 'code_name': 'category_dashboard'},
        {'name': 'Data Upload', 'code_name': 'upload_data'},
    ]

    for fd in features_data:
        Feature.objects.get_or_create(code_name=fd['code_name'], defaults={'name': fd['name']})

    old_email = 'devarthnanavaty1109@gmal.com'
    email = 'devarthnanavaty1109@gmail.com'
    
    # Update if the old typo exists
    Users.objects.filter(email=old_email).update(email=email)

    main_user, created = Users.objects.get_or_create(
        email=email,
        defaults={
            'fname': 'Devarth',
            'lname': 'Nanavaty',
            'pswd': 'Drn11@2003',
            'cpswd': 'Drn11@2003',
        }
    )
    
    if not created:
        main_user.pswd = 'Drn11@2003'
        main_user.cpswd = 'Drn11@2003'

    main_user.is_main_user = True
    main_user.save()
    
    all_features = Feature.objects.all()
    admin_role, _ = Role.objects.get_or_create(name='Admin', created_by=main_user)
    admin_role.features.set(all_features)
    
    print("RBAC Initialization Complete")
    
if __name__ == '__main__':
    init_rbac()
