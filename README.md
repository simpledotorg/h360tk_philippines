In order to launch this system:

```
docker compose up -d
```

Once the system is running, access the dashboard at:

- **URL:** http://localhost:3000/d/heart360demo/heart-360-global-dashboard
- **Username:** `admin`
- **Password:** `Admin@Heart360`

### Upload Files

To upload files, navigate to:

- **URL:** http://localhost:8080/
- **Username:** `admin`
- **Password:** `admin`

**Note:** We recommend using **CSV** files for data import, as they provide faster ingestion. **Excel (.xlsx)** files are also supported but may take longer to process.

### Important Security Note

⚠️ **The credentials provided above are default credentials.** They should be changed in the `docker-compose.yml` file after cloning the repository for security purposes.

