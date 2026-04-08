from functools import wraps
from django.shortcuts import redirect
from .models import Users

def require_feature(feature_code):
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(arg, *args, **kwargs):
            # arg is request for FBV, self for CBV method
            request = arg if hasattr(arg, 'session') else args[0]
            user_id = request.session.get('user_id')
            if not user_id:
                return redirect('account-login')
            
            try:
                user = Users.objects.get(id=user_id)
            except Users.DoesNotExist:
                return redirect('account-login')

            if user.is_main_user:
                return view_func(request, *args, **kwargs)

            # Sub-user logic
            if not user.role:
                return redirect('dashboard-home') # Or some error page
            
            has_feature = user.role.features.filter(code_name=feature_code).exists()
            if not has_feature:
                # Redirect to dashboard home if they lack access
                # For dashboard home itself, maybe we shouldn't redirect there if it's protected?
                # Usually home is accessible or routes to a permitted dashboard.
                # Here we just redirect to general dashboard
                return redirect('dashboard-home')

            return view_func(arg, *args, **kwargs)
        return _wrapped_view
    return decorator

def main_user_required(view_func):
    @wraps(view_func)
    def _wrapped_view(arg, *args, **kwargs):
        # arg is request for FBV, self for CBV method
        request = arg if hasattr(arg, 'session') else args[0]
        user_id = request.session.get('user_id')
        if not user_id:
            return redirect('account-login')
        try:
            user = Users.objects.get(id=user_id)
            if not user.is_main_user:
                return redirect('dashboard-home')
            request.user = user  # convenience
            return view_func(arg, *args, **kwargs)
        except Users.DoesNotExist:
            return redirect('account-login')
    return _wrapped_view

