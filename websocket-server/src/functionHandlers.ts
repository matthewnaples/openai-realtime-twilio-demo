import { FunctionHandler } from "./types";
import twilio from 'twilio';
import dotenv from 'dotenv';
import { closeAllConnections, closeModel } from "./sessionManager";

// Load environment variables
dotenv.config();

// Debug: Log environment variable status (but not their values for security)
console.log('Environment variables status:', {
  TWILIO_ACCOUNT_SID: !!process.env.TWILIO_ACCOUNT_SID,
  TWILIO_AUTH_TOKEN: !!process.env.TWILIO_AUTH_TOKEN,
  TWILIO_PHONE_NUMBER: !!process.env.TWILIO_PHONE_NUMBER
});

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const twilioPhoneNumber = process.env.TWILIO_PHONE_NUMBER;

// Initialize Twilio client only if credentials are present
let twilioClient: twilio.Twilio | null = null;
if (accountSid && authToken) {
  console.log('Initializing Twilio client...');
  twilioClient = twilio(accountSid, authToken);
  console.log('Twilio client initialized successfully');
} else {
  console.error('Missing required Twilio credentials (TWILIO_ACCOUNT_SID and/or TWILIO_AUTH_TOKEN)');
}

const functions: FunctionHandler[] = [];

functions.push({
  schema: {
    name: "get_weather_from_coords",
    type: "function",
    // strict:true, 
    description: "Get the current weather",
    parameters: {
      type: "object",
      properties: {
        latitude: {
          type: "number",
        },
        longitude: {
          type: "number",
        },
      },
      required: ["latitude", "longitude"],
    },
  },
  handler: async (args: { latitude: number; longitude: number }) => {
    console.log("[get_weather_from_coords] Handler called with args:", args);
    try {
      if (typeof args.latitude !== 'number' || typeof args.longitude !== 'number') {
        console.error("[get_weather_from_coords] Invalid arguments:", args);
        return JSON.stringify({ error: "Invalid latitude or longitude provided." });
      }
      const url = `https://api.open-meteo.com/v1/forecast?latitude=${args.latitude}&longitude=${args.longitude}&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m`;
      console.log("[get_weather_from_coords] Fetching weather from:", url);
      const response = await fetch(url);
      console.log("[get_weather_from_coords] Fetch response status:", response.status);
      if (!response.ok) {
        console.error("[get_weather_from_coords] Fetch failed:", response.status, response.statusText);
        return JSON.stringify({ error: `Weather API error: ${response.status} ${response.statusText}` });
      }
      const data = await response.json();
      console.log("[get_weather_from_coords] API response:", data);
      const currentTemp = data.current?.temperature_2m;
      if (typeof currentTemp === 'undefined') {
        console.error("[get_weather_from_coords] No temperature data in response:", data);
        return JSON.stringify({ error: "No temperature data found in API response." });
      }
      return JSON.stringify({ temp: currentTemp });
    } catch (err) {
      console.error("[get_weather_from_coords] Exception:", err);
      return JSON.stringify({ error: err instanceof Error ? err.message : String(err) });
    }
  },
});

functions.push({
  schema: {
    name: "add_participant_to_call",
    type: "function",
    // strict:true,
    description: "Add a new participant to the current conference call",
    parameters: {
      type: "object",
      properties: {
        phoneNumber: {
          type: "string",
          description: "The phone number to call and add to the conference (E.164 format)",
        },
      },
      required: ["phoneNumber"],
    },
  },
  handler: async (args: { phoneNumber: string }) => {
    console.log("add_participant_to_call has been called with args:", args);
    try {
      // Check if Twilio client is properly initialized
      if (!twilioClient) {
        throw new Error('Twilio client not initialized. Check your credentials.');
      }

      // Check if we have a phone number to call from
      if (!twilioPhoneNumber) {
        throw new Error('TWILIO_PHONE_NUMBER environment variable is not set');
      }

      const call = await twilioClient.calls.create({
        to: args.phoneNumber,
        from: twilioPhoneNumber,
        twiml: `<?xml version="1.0" encoding="UTF-8"?>
                <Response>
                    <Say>You are being added to a conference call</Say>
                    <Dial>
                        <Conference>conference-room</Conference>
                    </Dial>
                </Response>`
      });

      return JSON.stringify({
        success: true,
        message: "Participant is being called and will be added to the conference",
        callSid: call.sid
      });
    } catch (error: any) {
      console.error('Error in add_participant_to_call:', error);
      return JSON.stringify({
        success: false,
        error: error?.message || 'Unknown error occurred',
        details: error?.code ? `Twilio Error Code: ${error.code}` : undefined
      });
    }
  },
});

functions.push({
  schema: {
    name: "list_commands",
    type: "function",
    // strict:true,
    description: "Returns a list of descriptions of all available commands to the console, and a user_friendly_message for TTS playback.",
    parameters: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  handler: async () => {
    console.log("list_commands has been called");
    const commands = functions.map(f => ({
      name: f.schema.name,
      description: f.schema.description || "No description"
    }));
    console.log("Available commands:", commands);
    return JSON.stringify({
      commands,
      user_friendly_message: `There are ${commands.length} available commands. For example: ${commands.map(c => c.name).join(", ")}.` 
    });
  },
});

functions.push({
  schema: {
    name: "drop_call",
    type: "function",
    // strict:true,
    description: "Drops the call and returns a confirmation message and a user_friendly_message for TTS playback.",
    parameters: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  handler: async () => {
    console.log("drop_call has been called");
    closeAllConnections();
    return JSON.stringify({
      success: true,
      message: "Call dropped successfully.",
      user_friendly_message: "The call has been ended. Goodbye!"
    });
  },
});

functions.push({
  schema: {
    name: "text_todo_list",
    type: "function",
    // strict:true,
    description: "Returns a fabricated to-do list and a user_friendly_message for TTS playback.",
    parameters: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  handler: async () => {
    console.log("text_todo_list has been called");
    const todoList = [
      "Call Alice about the project update",
      "Send the signed contract to Bob",
      "Review the quarterly report",
      "Schedule a team meeting for Friday"
    ];
    return JSON.stringify({
      todo_list: todoList,
      user_friendly_message: `Here is your to-do list: ${todoList.join(", ")}. Would you like to add or remove anything?`
    });
  },
});

functions.push({
  schema: {
    name: "disconnect_from_voice_call",
    type: "function",
    description: "Disconnnects OpenAI from the voice call. This should only be called when the user asks the bot to stop talking. Returns a confirmation and a user_friendly_message for TTS playback.",
    parameters: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  handler: async () => {
    console.log("disconnect has been called");
    closeModel();
    return JSON.stringify({
      success: true,
      message: "Assistant has stopped talking. The OpenAI session is closed.",
      user_friendly_message: "Okay, I will stop talking now."
    });
  },
});

// functions.push({
//   schema: {
//     name: "do_nothing",
//     type: "function",
//     description: "Default function to be called when none of the other functions are applicable or asked for by the user. This function does nothing and is used when the model must call a function but no specific action is required.",
//     parameters: {
//       type: "object",
//       properties: {},
//       required: [],
//     },
//   },
//   handler: async () => {
//     console.log("do_nothing has been called");
//     return JSON.stringify({
//       success: true,
//       message: "No action needed",
//       user_friendly_message: ""  // Empty string since we don't want any TTS response
//     });
//   },
// });

export default functions;
