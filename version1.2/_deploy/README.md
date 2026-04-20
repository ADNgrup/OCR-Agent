# KVM OCR Project Deployment

## 1. Project Structure
- `backend/`: FastAPI application
- `frontend/`: Next.js application
- `_deploy/`: Docker Compose configuration for production

## 2. Prerequisites
- Docker & Docker Compose installed
- Network access to the LLM API (`LLM_BASEAPI`)

## 3. Configuration
All environment variables are managed in `_deploy/.env`.

Key configurations to check:
- `MONGO_INITDB_ROOT_USERNAME/PASSWORD`: Credentials for the database.
- `LLM_BASEAPI`: The endpoint for the LLM service.
- `NEXT_PUBLIC_BACKEND_URL`: Should be the public/server URL of the backend (e.g., `http://your-server-ip:8000`).

## 4. Deployment Steps

### Step 1: Navigate to deploy directory
```bash
cd _deploy
```

### Step 2: Build and start containers
```bash
docker compose up -d --build
```

### Step 3: Verify services
- **Backend API**: `http://<server-ip>:8000/docs`
- **Frontend App**: `http://<server-ip>:3000`
- **MongoDB**: `localhost:27017` (Internal auth enabled)

## 5. Monitoring
To check the logs:
```bash
docker compose logs -f
```

To stop the services:
```bash
docker compose down
```
