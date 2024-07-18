# Smart Analytics

## Project Description

This is a FastAPI project that serves as a template for building a high-performance API using Python's FastAPI framework. This project is designed to handle up to 1 million requests efficiently and can be used as a starting point for more complex applications.

## Features

- Fast and asynchronous API with FastAPI
- PostgreSQL database for data storage
- Redis for caching and session management
- Docker support for containerization
- Automatic interactive API documentation

## Installation

### Prerequisites

- Python 3.10+
- PostgreSQL
- Redis

### Setup

1. **Clone the repository:**

    ```bash
    git clone git@github.com:Pro100-Almaz/smart_analytics.git
    cd fastapi-project
    ```

2. **Create and activate a virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install the dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4. **Configure environment variables:**

    Create a `.env` file in the root directory and add your configurations:

    ```env
    DATABASE_URL=postgresql://user:password@localhost/dbname
    REDIS_URL=redis://localhost
    ```

5. **Run database migrations:**
    
    Coming soon. If further development will require the migrations history, 
    this library and architecture will be added. **But for now DON'T install!**

    ```bash
    alembic upgrade head
    ```

## Usage

### Running the API

1. **Start the FastAPI server:**

    ```bash
    uvicorn app.main:app --reload
    ```

2. **Access the API documentation:**

    Open your browser and go to `http://127.0.0.1:8000/docs` to see the interactive API documentation. Or you can use
    **nginx** for https connection, but path will remain the same!

## Testing

To run tests, use the following command:

    ```bash
    pytest
    ```    