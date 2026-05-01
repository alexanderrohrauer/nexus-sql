from app.models import Work
from app.utils.api_utils import SearchAndFilterParams


class WorkSearchParams(SearchAndFilterParams):
    def get_search_conditions(self, model_class) -> list:
        if self.search:
            return [model_class.title.ilike(f"%{self.search}%")]
        return []