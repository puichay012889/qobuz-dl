import requests


class AuthenticationError(Exception):
    pass


class IneligibleError(Exception):
    pass


class InvalidAppIdError(Exception):
    pass


class InvalidAppSecretError(Exception):
    pass


class InvalidQuality(Exception):
    pass


class NonStreamable(Exception):
    pass


class QobuzApiError(requests.exceptions.HTTPError):
    """Rich HTTP/API error with categorized reason details."""

    def __init__(
        self,
        endpoint,
        status_code,
        category,
        description,
        api_message=None,
        api_code=None,
        response=None,
    ):
        self.endpoint = endpoint
        self.status_code = status_code
        self.category = category
        self.description = description
        self.api_message = api_message
        self.api_code = api_code
        self.response = response
        message = self.format_message()
        super().__init__(message, response=response)

    def format_message(self):
        bits = [
            f"HTTP {self.status_code}",
            self.category,
            self.description,
            f"endpoint={self.endpoint}",
        ]
        if self.api_code is not None:
            bits.append(f"api_code={self.api_code}")
        if self.api_message:
            bits.append(f"api_message={self.api_message}")
        return " | ".join(bits)
