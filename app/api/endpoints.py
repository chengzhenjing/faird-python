from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import pyarrow as pa

router = APIRouter()

@router.post("/user/register")
async def user_register():
    """注册用户"""
    pass

@router.post("/user/login")
async def user_login():
    """用户登录"""
    pass