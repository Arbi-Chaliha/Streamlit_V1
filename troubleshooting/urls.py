from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('failures/', views.FailureListView.as_view(), name='failures'),
    path('metadata/', views.MetadataView.as_view(), name='metadata'),
    path('partition/', views.PartitionView.as_view(), name='partition'),
    path('checks/', views.ChecksView.as_view(), name='checks'),
    path('rootcause/', views.RootCauseView.as_view(), name='rootcause'),
    path('graph/', views.GraphView.as_view(), name='graph'),
]