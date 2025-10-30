class GithubReposScrapperError(Exception):
    message: str = "There was an error in work of GithubReposScrapper."

    def __init__(self, message: str = None):
        super().__init__(message or self.message)


class GithubNotAvailableError(GithubReposScrapperError):
    message: str = "Github is not available at the moment."

