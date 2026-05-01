from app.models import Institution
from app.utils.api_utils import SearchAndFilterParams


class InstitutionSearchParams(SearchAndFilterParams):
    def get_search_conditions(self, model_class) -> list:
        if self.search:
            return [model_class.name.ilike(f"%{self.search}%")]
        return []