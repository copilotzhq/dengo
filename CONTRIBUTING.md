# Contributing to Dengo

Thank you for considering contributing to Dengo! This document provides guidelines and instructions for contributing to the project.

## 🌟 Ways to Contribute

There are many ways to contribute to Dengo:

1. **Code Contributions**: Implement new features or fix bugs
2. **Documentation**: Improve or expand documentation
3. **Examples**: Create example applications using Dengo
4. **Testing**: Write tests or identify edge cases
5. **Bug Reports**: Submit detailed bug reports
6. **Feature Requests**: Suggest new features or improvements
7. **Spread the Word**: Star the repository, share it with others

## 🚀 Getting Started

### Development Environment Setup

1. **Fork and Clone the Repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/dengo.git
   cd dengo
   ```

2. **Run Tests**
   ```bash
   deno test --unstable-kv
   ```

3. **Run Examples**
   ```bash
   deno run --unstable-kv examples/todo-app/mod.ts
   ```

## 📝 Pull Request Process

1. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Your Changes**
   - Follow the coding style of the project
   - Add or update tests as necessary
   - Update documentation to reflect your changes

3. **Run Tests**
   ```bash
   deno test --unstable-kv
   ```

4. **Update MONGODB_COMPAT.md**
   - If you've implemented or modified a feature, update the compatibility document
   - Mark newly implemented features with [x]
   - Document any limitations or differences from MongoDB

5. **Commit Your Changes**
   ```bash
   git commit -m "feat: add some feature"
   ```
   We follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages.

6. **Push to Your Fork**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Submit a Pull Request**
   - Fill in the pull request template
   - Reference any related issues
   - Describe your changes in detail

## 📋 Code Style Guidelines

- Use TypeScript for all code
- Follow the existing code style
- Use meaningful variable and function names
- Add comments for complex logic
- Keep functions small and focused

## 🐛 Reporting Bugs

When reporting bugs, please include:

1. **Description**: Clear description of the bug
2. **Steps to Reproduce**: Detailed steps to reproduce the issue
3. **Expected Behavior**: What you expected to happen
4. **Actual Behavior**: What actually happened
5. **Environment**: Deno version, OS, etc.
6. **Code Sample**: Minimal code sample that reproduces the issue

## 💡 Feature Requests

When suggesting features, please include:

1. **Description**: Clear description of the feature
2. **Use Case**: Why this feature would be useful
3. **Proposed Implementation**: If you have ideas on how to implement it
4. **MongoDB Compatibility**: How this relates to MongoDB's API

## 🔍 Code Review Process

All submissions require review before being merged:

1. At least one maintainer must approve the changes
2. All tests must pass
3. Documentation must be updated
4. MONGODB_COMPAT.md must be updated if applicable

## 📊 Project Structure

```
dengo/
├── mod.ts              # Main entry point
├── MONGODB_COMPAT.md   # MongoDB compatibility documentation
├── examples/           # Example applications
│   ├── todo-app/       # Todo application example
│   └── ...
├── src/                # Source code
│   ├── collection.ts   # Collection implementation
│   ├── database.ts     # Database implementation
│   ├── objectid.ts     # ObjectId implementation
│   └── ...
└── tests/              # Tests
    ├── collection_test.ts
    └── ...
```

## 🙏 Thank You

Your contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

---

<div align="center">
  <sub>Happy coding! ❤️</sub>
</div> 