
from django.urls import path
from . import views

app_name = 'troubleshooting'

urlpatterns = [
    # Main troubleshooting interface
    path('', views.index, name='index'),
    
    # API endpoints for AJAX calls
    path('api/job-numbers/', views.get_job_numbers, name='get_job_numbers'),
    path('api/start-jobs/', views.get_start_jobs, name='get_start_jobs'),
    path('api/analyze-failure/', views.analyze_failure, name='analyze_failure'),
     # Analysis endpoint
   
]