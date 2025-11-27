from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import CompanyViewSet, CnaesViewSet, MunicipiosViewSet

router = DefaultRouter()
router.register(r'companies', CompanyViewSet, basename='company')
router.register(r'cnaes', CnaesViewSet, basename='cnaes')
router.register(r'municipios', MunicipiosViewSet, basename='municipios')

urlpatterns = [
    path('', include(router.urls)),
]
