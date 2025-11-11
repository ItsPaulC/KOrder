// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace KOrder.Api.Extension;

public static class ConfigurationExtensions
{
    /// <summary>
    /// Binds a configuration section to a strongly-typed settings object.
    /// </summary>
    /// <typeparam name="T">The type of settings class to bind to.</typeparam>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="sectionName">Optional section name. If null, uses the type name without "Settings" suffix.</param>
    /// <returns>A populated instance of type T, or a new instance if the section doesn't exist.</returns>
    /// <exception cref="InvalidOperationException">Thrown when required properties are not populated.</exception>
    public static T GetConfigSection<T>(
        this IConfiguration configuration,
        string? sectionName = null) where T : class, new()
    {
        // Use the type name as default section name (remove "Settings" suffix if present)
        sectionName ??= typeof(T).Name;

        IConfigurationSection section = configuration.GetSection(sectionName);
        T settings = section.Get<T>() ?? new T();

        ValidateRequiredProperties(settings, sectionName);

        return settings;
    }

    /// <summary>
    /// Binds a configuration section to a settings object and returns it via out parameter.
    /// </summary>
    /// <typeparam name="T">The type of settings class to bind to.</typeparam>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="settings">The output parameter that receives the populated settings instance.</param>
    /// <param name="sectionName">Optional section name. If null, uses the type name without "Settings" suffix.</param>
    /// <exception cref="InvalidOperationException">Thrown when required properties are not populated.</exception>
    public static void BindConfigSection<T>(
        this IConfiguration configuration,
        out T settings,
        string? sectionName = null) where T : class
    {
        sectionName ??= typeof(T).Name;

        IConfigurationSection section = configuration.GetSection(sectionName);
        settings = section.Get<T>() ?? throw new ArgumentException("Configuration section not found", nameof(sectionName));

        ValidateRequiredProperties(settings, sectionName);
    }

    /// <summary>
    /// Validates that all properties marked with [Required] or [RequiredMember] attribute are populated.
    /// </summary>
    /// <typeparam name="T">The type of settings object to validate.</typeparam>
    /// <param name="settings">The settings object to validate.</param>
    /// <param name="sectionName">The configuration section name (for error messages).</param>
    /// <exception cref="InvalidOperationException">Thrown when required properties are not populated.</exception>
    private static void ValidateRequiredProperties<T>(T settings, string sectionName) where T : class
    {
        List<string> missingProperties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetCustomAttribute<RequiredMemberAttribute>() is not null ||
                       p.GetCustomAttribute<RequiredAttribute>() is not null)
            .Where(p => IsPropertyValueInvalid(p.GetValue(settings)))
            .Select(p => p.Name)
            .ToList();

        if (missingProperties.Count > 0)
        {
            string propertyList = string.Join(", ", missingProperties);
            throw new InvalidOperationException(
                $"Configuration section '{sectionName}' is missing required properties: {propertyList}. " +
                $"Please ensure these values are set in your appsettings.json or environment variables.");
        }
    }

    /// <summary>
    /// Determines if a property value is invalid (null or empty/whitespace for strings).
    /// </summary>
    /// <param name="value">The property value to check.</param>
    /// <returns>True if the value is invalid; otherwise, false.</returns>
    private static bool IsPropertyValueInvalid(object? value) =>
        value is null || (value is string stringValue && string.IsNullOrWhiteSpace(stringValue));
}
