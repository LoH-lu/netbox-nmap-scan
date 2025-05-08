# Contributing to NetBox Nmap Scan

Thank you for considering contributing to NetBox Nmap Scan!

This project is a plugin for [NetBox](https://github.com/netbox-community/netbox) that integrates Nmap scanning functionality directly into the NetBox environment. Your contributions help make the project better for everyone.

## How to Contribute

There are several ways you can contribute:

- **Bug Reports**: If you find a bug, please open an issue and provide clear steps to reproduce it.
- **Feature Requests**: Got an idea to improve the plugin? Open an issue with a `[Feature]` tag and describe your use case.
- **Code Contributions**: Fork the repo, make your changes in a new branch, and submit a Pull Request (PR).
- **Documentation Improvements**: Typos or unclear docs? Feel free to help improve the documentation.

## Development Environment Setup

To set up the project locally for development:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/LoH-lu/netbox-nmap-scan.git
   cd netbox-nmap-scan
   ```

2. **Install dependencies:**
   It's recommended to use a virtual environment.
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Set up in NetBox:**
   Follow the installation instructions in the `README.md` to integrate the plugin into your local NetBox instance.

## Code Style

Please follow these conventions:

- Follow [PEP8](https://www.python.org/dev/peps/pep-0008/) for Python code.
- Use 4 spaces for indentation (no tabs).
- Include docstrings and inline comments where appropriate.
- Ensure code is linted using tools like `flake8` or `black` before submitting a PR.

## Pull Request Guidelines

- Create a feature branch from `main`:
  ```bash
  git checkout -b feature/my-feature
  ```

- Make sure your code is tested and doesn’t break existing functionality.
- Write clear commit messages and include related issue numbers if applicable.
- Open a Pull Request with a description of what you’ve done and why.

## Reporting Issues

If you encounter a problem:

- Search existing issues before opening a new one.
- Provide steps to reproduce the issue, expected behavior, and actual behavior.
- Include version numbers for NetBox, Python, and the plugin itself.

## License

By contributing to this repository, you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).

---

*Thank you for helping make NetBox Nmap Scan better!*  
