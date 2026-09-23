# Contributing to AWS MSK Kafka Starter

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for all contributors.

## How to Contribute

### Reporting Issues

- Check if the issue already exists in the [issue tracker](https://github.com/saranreddy/aws-msk-kafka-starter/issues)
- Use a clear, descriptive title
- Include steps to reproduce the issue
- Provide your environment details (OS, Terraform version, Python version, AWS region)

### Suggesting Enhancements

- Open an issue with the label "enhancement"
- Clearly describe the feature and its benefits
- Include example use cases

### Pull Requests

1. **Fork the repository** and create your branch from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Follow existing code style
   - Add tests if applicable
   - Update documentation

3. **Test your changes**
   ```bash
   make test
   ```

4. **Commit your changes**
   - Use clear, descriptive commit messages
   - Reference related issues (e.g., "Fixes #123")
   ```bash
   git commit -m "Add feature: your feature description"
   ```

5. **Push and create a pull request**
   ```bash
   git push origin feature/your-feature-name
   ```

## Development Setup

1. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/aws-msk-kafka-starter.git
   cd aws-msk-kafka-starter
   ```

2. Install development dependencies:
   ```bash
   pip install -r requirements.txt
   pip install black isort pylint
   ```

3. Install pre-commit hooks (optional):
   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Coding Standards

### Terraform

- Use Terraform 1.5.0 or newer
- Run `terraform fmt` before committing
- Use meaningful resource names
- Add comments for complex logic
- Follow [Terraform Best Practices](https://www.terraform.io/docs/cloud/guides/recommended-practices/index.html)

### Python

- Follow [PEP 8](https://pep8.org/) style guide
- Use type hints where applicable
- Add docstrings to functions and classes
- Use `black` for formatting (line length: 100)
- Use `isort` for import sorting
- Handle exceptions appropriately

### Documentation

- Update README.md for significant changes
- Keep code comments up to date
- Use clear, concise language
- Include examples where helpful

## Testing

Before submitting a PR, ensure:

- [ ] Terraform code is formatted: `make format`
- [ ] Python code is formatted: `black scripts/`
- [ ] All linters pass: `make lint`
- [ ] Terraform validates: `cd infra && terraform validate`
- [ ] Python scripts compile: `python -m py_compile scripts/*.py`
- [ ] Manual testing completed (if applicable)

## Pull Request Process

1. Update the README.md with details of changes if needed
2. Ensure CI checks pass
3. Request review from maintainers
4. Address review feedback
5. Once approved, a maintainer will merge your PR

## Questions?

Feel free to open an issue with the "question" label if you need help or clarification.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
