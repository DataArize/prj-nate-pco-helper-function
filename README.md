# Helper Columns Data Transformation and BigQuery Integration

This repository provides a Python-based system for transforming and computing helper columns used for analysis. It handles complex calculations for subscription and appointment data, simplifying the process for downstream analysis by creating helper columns and inserting the processed data into BigQuery tables.

---

## Overview:

The project splits the complex calculations for the `subscription` and `appointment` tables into dedicated transformations. By doing so, it enables easier and more efficient handling of calculations in future workflows.

### Key Functionalities:

1. **Helper Column Computation:**

   - Computes helper columns for each subscription or appointment, providing pre-processed data for analysis.

2. **Data Transformation:**

   - Transforms raw data into a format that is ready for BigQuery.

3. **Data Validation:**

   - Ensures the required columns are present and validates data types for each column.

4. **BigQuery Integration:**
   - Inserts the processed data back into BigQuery for downstream usage.

### Example Workflow:

1. Extract raw data for `subscription` or `appointment` from the source.
2. Validate and transform the data using predefined rules.
3. Compute helper columns specific to subscriptions and appointments.
4. Insert the transformed data into BigQuery for analysis.

---

## Prerequisites:

- **Python**: Ensure Python 3.11 or later is installed on your system. Install it from the official Python website.
- **Google Cloud Account**: You need a Google Cloud account with permissions to manage BigQuery tables.
- **Service Account**: A service account with roles like `roles/bigquery.admin` and `roles/storage.objectViewer` is required.
- **BigQuery Dataset**: Ensure a BigQuery dataset is already created to load the data.
- **GitHub Secrets**: Ensure the required secrets are set up in your GitHub repository for the CI/CD pipeline.

---

## Setup

### Clone the repository:

```bash
  git clone git@github.com:<your-organization>/<your-repository>.git
  cd <your-repository>
```

### Configure Python Environment:

1. Create and activate a virtual environment:

```bash
  python -m venv venv
  source venv/bin/activate
```

2. Install dependencies:

```bash
  pip install -r requirements.txt
```

### Configure Google Cloud Credentials

You need to authenticate with Google Cloud by setting up your service account. Export your credentials file as an environment variable:

```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
```

---

## CI/CD Pipeline for Deployment

This repository is configured with a CI/CD pipeline to deploy the code as a Google Cloud Function.

### Deployment Process:

1. **Pipeline Triggers:**

   - Pushes or pull requests to the `main` and `develop` branches.
   - Manually triggered deployments via `workflow_dispatch`.

2. **Pipeline Jobs:**

   - **Security Scan:** Runs `Trivy` and `Safety` to scan for vulnerabilities in dependencies.
   - **Testing:** Runs `pytest` with code coverage.
   - **Deploy to Environment:** Deploys the code to Google Cloud Function (`production` or `QA`) based on the branch.

3. **GitHub Secrets Required:**

   - `PRODUCTION_GCP_PROJECT_ID` / `DEVELOPMENT_GCP_PROJECT_ID`
   - `PRODUCTION_GCP_SERVICE_ACCOUNT` / `DEVELOPMENT_GCP_SERVICE_ACCOUNT`
   - `PRODUCTION_GCP_WORKLOAD_IDENTITY_PROVIDER` / `DEVELOPMENT_GCP_WORKLOAD_IDENTITY_PROVIDER`

4. **Manual Deployment Confirmation:**

   - For manual triggers, type "yes" in the `workflow_dispatch` input to confirm deployment.

5. **Command Example:** To deploy the function:

```bash
gcloud functions deploy helper-columns-transformation \
    --runtime python311 \
    --trigger-http \
    --allow-unauthenticated \
    --entry-point perform_transformation \
    --region us-central1 \
    --set-env-vars "<your-environment-variables>" \
    --service-account=<your-service-account>
```

---

## Testing

Run tests using the following command:

```bash
  pytest tests/
```

---

## Future Enhancements

- Add support for more helper column computations.
- Extend CI/CD pipeline to deploy updates to staging, QA, and production environments based on branch rules.
- Include Terraform scripts for managing infrastructure alongside the library.

---

## License

This project is licensed under the MIT License.
