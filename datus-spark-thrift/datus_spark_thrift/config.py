# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class SparkThriftConfig(BaseModel):
    """Spark Thrift Server configuration."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    host: str = Field(..., description="Spark Thrift Server host")
    port: int = Field(default=10000, description="Thrift server port")
    username: str = Field(default="", description="Username for authentication")
    password: str = Field(default="", description="Password for authentication")
    database: Optional[str] = Field(default=None, description="Default database name")
    auth: str = Field(default="LDAP", description="Authentication mechanism: NONE, CUSTOM, LDAP")
    timeout_seconds: int = Field(default=30, description="Connection timeout in seconds")
