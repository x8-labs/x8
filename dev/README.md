# Development Setup Guide

This guide provides step-by-step instructions to set up your development environment for working with **x8**.

---

## 1. Install Python

Ensure that **Python 3.11 or later** is installed.

### Steps:

1. Download Python from [python.org](https://www.python.org/downloads/) and run the installer.
2. During installation, **ensure** the following options are checked:
   - ✅ **Add Python to PATH** (should be checked by default)
   - ✅ **Install pip**
3. Verify the installation:

   - **Windows**:
     ```sh
     python --version
     ```
   - **Linux / macOS**:
     ```sh
     python3 --version
     ```

   In this guide, `python` and `python3` are used interchangeably. Use the appropriate command based on your OS.

---

## 2. Install uv

**uv** is used for dependency management and virtual environment creation.

### Steps:

1. Install uv by following the instructions at [docs.astral.sh](https://docs.astral.sh/uv/getting-started/installation/).
2. Verify the installation:
   ```sh
   uv --version
   ```

---

## 3. Clone the Repository

Navigate to that directory and clone the repository:

```sh
git clone https://github.com/x8-labs/x8.git
```

This will create a folder named `x8`.

---

## 4. Install Visual Studio Code (VS Code)

1. Download and install **VS Code** from [code.visualstudio.com](https://code.visualstudio.com/download).
2. Open VS Code.
3. Open the repository:
   - **Windows / Linux / macOS**:  
     Go to **File → Open Folder**, and select the `x8` directory.

---

## 5. Import VS Code Profile

The repository contains a pre-configured **VS Code profile** with necessary extensions.

### Steps:

1. Open VS Code and click on the **settings** button (bottom-left corner).
2. Select **Profile → Import Profile → Select File**.
3. Choose the file:
   ```
   dev/x8.code-profile
   ```
4. The workspace settings from `.vscode/settings.json` will be applied automatically.

This setup ensures that formatting, linting, type checking, and styling configurations are applied.

---

## 6. Set Up Env and Install Dependencies

**uv** is used to manage virtual environments.

### Steps:

1. Open a terminal inside VS Code (**Terminal → New Terminal**).
2. Navigate to the `x8` directory (if not already there).
3. From the repo root:

   ```sh
   uv sync --extra all --extra dev
   ```

---

## 7. Run Sample Code

To verify the setup, run a sample test:

```sh
uv run python -m playground.test
```

If **"SUCCESS"** is printed at the end, the setup is complete.

---

# Advanced Development Setup

To run tests and access **Google Cloud Project** test resources, follow these steps.

---

## 1. Get Access

Permission is required to use the **X8 Test Google Cloud Project**.

- Contact **Lenin** for access.

---

## 2. Install Google Cloud SDK

1. Follow the official guide to install the **Google Cloud SDK**:  
   [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
2. Authenticate and configure the default project:

   ```sh
   gcloud init
   gcloud auth application-default login
   ```

3. Select **`verse-test-427017`** as the default project.

---

## 3. Run Tests

To verify test execution, run the following:

```sh
cd x8/tests
uv run python -m pytest ./storage/object_store -k test_put
```

If the test runs successfully, the setup is correct.

---

## 🎉 You’re all set!

Now you can start developing and contributing to **X8**! 🚀
