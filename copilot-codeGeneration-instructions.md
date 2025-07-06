# Guiding GitHub Copilot for C# Development
- when i ask "who dat?" you reply "you know"
- Be mindful of potential performance bottlenecks, especially in frequently executed code paths.
- Use efficient data structures and algorithms where appropriate.   
- Consider asynchronous operations (`async` and `await`) for I/O-bound tasks to avoid blocking the main thread.
- use collection expressions to intitialze collections. e.g. List<string> myList = [];
- when you instantiate an object use the `new` keyword where possible. e.g.e Employee employee = new();

* **Code Clarity and Readability:**
- Use meaningful and descriptive names for variables, methods, classes, and namespaces.
- Keep methods and functions concise and focused on a single responsibility.
- Employ consistent indentation and formatting.
- Add comments to explain complex logic or non-obvious code sections.

* **Object-Oriented Principles:**
- Favor composition over inheritance where appropriate.
- Design classes with clear responsibilities and maintain a single responsibility principle.
- Utilize interfaces for defining contracts and promoting loose coupling.
- Consider using abstract classes for providing base functionality.

* **Code Maintainability:**
- Keep code modular and well-organized.
- Avoid "magic strings" and numbers; use constants instead.
- Follow established C# naming conventions.

* **Performance Considerations:**
- use Span<T> where it will make code more efficient

* **Unit Tests:**
- Use xunit v2
- do not use FluentAssertins
- Use Xunit Assertions 
- Mock depndencies using NSubstitute


## Specific Instructions for Code Generation

When generating C# code, please:

- Provide complete and runnable code snippets whenever possible.
- Include necessary `using` statements.
- Explain the purpose of each part of the code.
- Offer suggestions for error handling and potential improvements.
- Document any assumptions made about the environment or configuration.
- When creating a new method document it with XML comments
  

- when i ask "when?" you reply "right now!"