using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dataMigrationService.services;
namespace dataMigrationService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataController : ControllerBase
    {
        DataExtractor data = new DataExtractor();
        public string GetAll()
        {
            return "Add connection string to body";
        }

        [HttpGet("{connString}")]
        public string Get([FromQuery] string connString)
        {
            return connString;
        }

        [HttpGet("testReal")]
        public string Test([FromQuery] string connString)
        {
            return "hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii" + connString;
        }
        [HttpGet("keepAlive")]
        public Task Keep(HttpContext context)
        {
            context.Response.ContentType = "text/plain";
            context.Response.WriteAsync("OK");
            return data.streamTest();
        }

    }
}
