from app.models import Researcher
from app.utils.api_utils import SearchAndFilterParams


class ResearcherSearchParams(SearchAndFilterParams):
    def get_search_conditions(self, model_class) -> list:
        if self.search:
            return [model_class.full_name.ilike(f"%{self.search}%")]
        return []