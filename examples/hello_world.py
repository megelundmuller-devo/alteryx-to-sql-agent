# A simple example showing how to use the project.

def greet(name: str) -> str:
    return f"Hello, {name}!"


if __name__ == "__main__":
    message = greet("World")
    print(message)
