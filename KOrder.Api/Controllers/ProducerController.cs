using Microsoft.AspNetCore.Mvc;
using KOrder.Api.Services;

namespace KOrder.Api.Controllers;

/// <summary>
/// Controller for managing Kafka message production
/// </summary>
[ApiController]
[Route("[controller]")]
public class ProducerController : ControllerBase
{
    private readonly IKafkaProducerService _kafkaProducerService;
    private readonly ILogger<ProducerController> _logger;

    /// <summary>
    /// Initializes a new instance of the ProducerController
    /// </summary>
    /// <param name="kafkaProducerService">The Kafka producer service</param>
    /// <param name="logger">The logger instance</param>
    public ProducerController(IKafkaProducerService kafkaProducerService, ILogger<ProducerController> logger)
    {
        _kafkaProducerService = kafkaProducerService;
        _logger = logger;
    }

    /// <summary>
    /// Runs a demo by producing predefined messages to Kafka with different keys
    /// </summary>
    /// <returns>A response indicating the demo completion status</returns>
    [HttpGet("Demo")]
    public async Task<IActionResult> Demo()
    {
        try
        {
            _logger.LogInformation("Starting demo message production");
            
            await _kafkaProducerService.RunDemoAsync();
            
            _logger.LogInformation("Demo completed successfully");
            
            return Ok(new 
            { 
                Message = "Demo completed successfully - sent messages for 3 different customers.",
                Details = "Check the consumer output - messages for each customer should be in order.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during demo execution");
            return StatusCode(500, new 
            { 
                Message = "An error occurred while running the demo",
                Error = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
    }
}
