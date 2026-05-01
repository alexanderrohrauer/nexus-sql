import random
from typing import Optional, Tuple

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession


async def require_instance(result):
    if result is not None:
        return result
    raise HTTPException(status_code=404, detail="This instance was not found!")


def fix_location_util(location) -> Optional[Tuple[float, float]]:
    if location is None:
        return None
    if isinstance(location, tuple):
        lon, lat = location
    else:
        from geoalchemy2.shape import to_shape
        point = to_shape(location)
        lon, lat = point.x, point.y
    return lon + random.uniform(0.0002, 0.0003), lat