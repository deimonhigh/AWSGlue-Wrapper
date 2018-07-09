from enum import Enum


class StatusETL(Enum):
	Ok = 200
	Error = 500
	CredentialsError = 401
