using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace dataMigrationService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataController : ControllerBase
    {
        public string GetAll()
        {
            return "Add connection string to body";
        }

        [HttpGet("{connString}")]
        public string Get([FromBody] string connString)
        {
            return connString;
        }
    }
}
