import random
from typing import List

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

app = FastAPI()
security = HTTPBearer()


class Device(BaseModel):
    id: int
    provisioning_state: str
    name: str


# Mock data: 200 devices, paginated (20/page), random states
all_devices = [
    Device(
        id=i,
        provisioning_state=random.choice(["ready", "pending", "failed"]),
        name=f"DGX-Node-{i}",
    )
    for i in range(1, 201)
]


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials  # <-- this works on ALL FastAPI versions
    if token != "fake-jwt-token":
        raise HTTPException(status_code=401, detail="Invalid token")
    return token


@app.get("/v1/devices")
def get_devices(
    page: int = 1,
    limit: int = 20,
    skip: int = None,
    credentials: str = Depends(get_current_user),
):
    if skip is None:
        skip = (page - 1) * limit
    devices = all_devices[skip : skip + limit]
    next_page = (
        f"/devices?page={page+1}&limit={limit}" if len(devices) == limit else None
    )
    return {
        "devices": [
            {"id": d.id, "provisioning_state": d.provisioning_state, "name": d.name}
            for d in devices
        ],
        "next_url": next_page,
        "total": len(all_devices),
        "page": page,
    }


@app.get("/v1/devices/{device_id}")
def get_device(device_id: int, token: str = Depends(get_current_user)):
    try:
        device = next(d for d in all_devices if d.id == device_id)
        return device.model_dump()  # or device.model_dump() in Pydantic v2
    except StopIteration:
        raise HTTPException(status_code=404, detail=f"Device {device_id} not found")


@app.get("/v1/health")
def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
