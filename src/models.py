import pyarrow as pa

from croniter import croniter
from pydantic import BaseModel, EmailStr, Field, model_validator, types
from typing import List, Optional, Self, TypedDict, Union


# ====== Job Schemas ====== #
class JobDefinition(TypedDict):
    url: Optional[str]
    cron_schedule: Optional[str]

    @model_validator(mode='after')
    def check_cron_expression(self) -> Self:
        cron_schedule = self.get('cron_schedule')
        if not croniter.is_valid(cron_schedule):
            raise ValueError(f"'{cron_schedule}' is not a valid cron expression")
        return self

class Jobs(TypedDict):
    users: Optional[JobDefinition]
    posts: Optional[JobDefinition]

class JobParameters(BaseModel):
    author_email: EmailStr
    author_name: Optional[str] = None
    jobs: Jobs


# ====== Post Schemas ====== #
class Post(BaseModel):
    userId: types.PositiveInt
    id: types.PositiveInt
    title: str = Field(min_length=2)
    body: str = Field(min_length=10)

class PostItems(BaseModel):
    items: List[Post]

    @classmethod
    def duckdb_schema(cls, table_name="posts"):
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                userId INTEGER NOT NULL,
                id INTEGER NOT NULL,
                title TEXT,
                body TEXT
            )
        """

    @classmethod
    def pyarrow_schema(cls):
        return pa.schema(
            [
                pa.field("userId", pa.uint32()),
                pa.field("id", pa.uint32()),
                pa.field("title", pa.string()),
                pa.field("body", pa.string()),
            ]
        )


# ====== Job Schemas ====== #
class Geo(BaseModel):
    lat: str
    lng: str

class Address(BaseModel):
    street: str
    suite: str
    city: str
    zipcode: str
    geo: Geo

class Company(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    catch_phrase: Optional[str] = None
    bs: str

class User(BaseModel):
    id: types.PositiveInt
    name: str
    username: str = Field(min_length=3, max_length=50)
    email: EmailStr 
    address: Address
    phone: str # As a improvement, validator and phone checker
    website: str
    company: Company

class UsersItems(BaseModel):
    items: List[User]
