import { RawData, WebSocket } from "ws";
import functions from "./functionHandlers";
import { createClient, LiveTranscriptionEvents } from "@deepgram/sdk";
import fs from 'fs';
import path from 'path';
// import { CallRecorder } from "./callRecorder";

interface Session {
  twilioConn?: WebSocket;
  frontendConn?: WebSocket;
  modelConn?: WebSocket;
  deepgramConn?: any;
  streamSid?: string;
  saved_config?: any;
  lastAssistantItem?: string;
  responseStartTimestamp?: number;
  latestMediaTimestamp?: number;
  openAIApiKey?: string;
  deepgramApiKey?: string;
  // recorder?: CallRecorder;
}

let session: Session = {};

function initializeDeepgramConnection() {
  if (!session.deepgramApiKey) return;

  const deepgram = createClient(session.deepgramApiKey);
  session.deepgramConn = deepgram.listen.live({
    model: "nova-2",
    smart_format: true,
    encoding: "mulaw",
    sample_rate: 8000,
    channels: 1,
    diarize: true,
    utterances: true
  });

  session.deepgramConn.on(LiveTranscriptionEvents.Open, () => {
    console.log("Deepgram connection opened");
  });

  session.deepgramConn.on(LiveTranscriptionEvents.Close, () => {
    console.log("Deepgram connection closed");
    session.deepgramConn = undefined;
  });

  session.deepgramConn.on(LiveTranscriptionEvents.Transcript, (transcription: any) => {
    if (transcription.channel?.alternatives?.[0]) {
      const firstAlternative = transcription.channel.alternatives[0];
      console.log("Deepgram transcription:", firstAlternative);
      if (session.frontendConn) {
        jsonSend(session.frontendConn, {
          type: "transcription",
          transcription: firstAlternative
        });
      }
    }
  });

  session.deepgramConn.on(LiveTranscriptionEvents.Error, (error: any) => {
    console.error("Deepgram error:", error);
  });
}

export function handleCallConnection(ws: WebSocket, openAIApiKey: string, deepgramApiKey: string) {
  cleanupConnection(session.twilioConn);
  session.twilioConn = ws;
  session.openAIApiKey = openAIApiKey;
  session.deepgramApiKey = deepgramApiKey;

  ws.on("message", handleTwilioMessage);
  ws.on("error", ws.close);
  ws.on("close", () => {
    cleanupConnection(session.modelConn);
    cleanupConnection(session.twilioConn);
    if (session.deepgramConn) {
      session.deepgramConn.finish();
      session.deepgramConn = undefined;
    }
    session.twilioConn = undefined;
    session.modelConn = undefined;
    session.streamSid = undefined;
    session.lastAssistantItem = undefined;
    session.responseStartTimestamp = undefined;
    session.latestMediaTimestamp = undefined;
    if (!session.frontendConn) session = {};
  });
}

export function handleFrontendConnection(ws: WebSocket) {
  cleanupConnection(session.frontendConn);
  session.frontendConn = ws;

  ws.on("message", handleFrontendMessage);
  ws.on("close", () => {
    cleanupConnection(session.frontendConn);
    session.frontendConn = undefined;
    if (!session.twilioConn && !session.modelConn) session = {};
  });
}

async function handleFunctionCall(item: { name: string; arguments: string }) {
  console.log("Handling function call:", item);
  const fnDef = functions.find((f) => f.schema.name === item.name);
  if (!fnDef) {
    throw new Error(`No handler found for function: ${item.name}`);
  }

  let args: unknown;
  try {
    args = JSON.parse(item.arguments);
  } catch {
    return JSON.stringify({
      error: "Invalid JSON arguments for function call.",
    });
  }

  try {
    console.log("Calling function:", fnDef.schema.name, args);
    const result = await fnDef.handler(args as any);
    return result;
  } catch (err: any) {
    console.error("Error running function:", err);
    return JSON.stringify({
      error: `Error running function ${item.name}: ${err.message}`,
    });
  }
}

function handleTwilioMessage(data: RawData) {
  const msg = parseMessage(data);
  if (!msg) return;

  switch (msg.event) {
    case "start":
      session.streamSid = msg.start.streamSid;
      session.latestMediaTimestamp = 0;
      session.lastAssistantItem = undefined;
      session.responseStartTimestamp = undefined;
      // if (session.streamSid) {
      //   session.recorder = new CallRecorder(session.streamSid);
      // }
      // initializeDeepgramConnection();
      tryConnectModel();
      break;
    case "media":
      session.latestMediaTimestamp = msg.media.timestamp;
      if (isOpen(session.modelConn)) {
        jsonSend(session.modelConn, {
          type: "input_audio_buffer.append",
          audio: msg.media.payload,
        });
      }
      if (session.deepgramConn) {
        const audio = Buffer.from(msg.media.payload, "base64");
        session.deepgramConn.send(audio);
      }
      // if (session.recorder) {
      //   session.recorder.write(msg.media.payload);
      // }
      break;
    case "close":
      // if (session.recorder) {
      //   session.recorder.finalize()
      //     .then(outputPath => {
      //       console.log(`Recording saved for call ${session.streamSid}: ${outputPath}`);
      //     })
      //     .catch(err => {
      //       console.error(`Error finalizing recording for call ${session.streamSid}:`, err);
      //     });
      // }
      closeAllConnections();
      break;
  }
}

function handleFrontendMessage(data: RawData) {
  const msg = parseMessage(data);
  if (!msg) return;

  if (isOpen(session.modelConn)) {
    jsonSend(session.modelConn, msg);
  }

  if (msg.type === "session.update") {
    console.log("session.update", msg.session);
    session.saved_config = msg.session;
  }
}

function tryConnectModel() {
  if (!session.twilioConn || !session.streamSid || !session.openAIApiKey)
    return;
  if (isOpen(session.modelConn)) return;
  session.modelConn = new WebSocket(
    "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
    {
      headers: {
        Authorization: `Bearer ${session.openAIApiKey}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  session.modelConn.on("open", () => {
    const config = session.saved_config || {};
    console.log("config", config)

    jsonSend(session.modelConn, {
      type: "session.update",
      session: {
        modalities: ["text"],
        turn_detection: { 
          type: "server_vad",
          create_response: true
        },
        voice: "coral",
        input_audio_transcription: { model: "whisper-1" },
        input_audio_format: "g711_ulaw",
        output_audio_format: "g711_ulaw",
        // tool_choice: "required",

        ...config,
      },
    });
  });

  session.modelConn.on("message", handleModelMessage);
  session.modelConn.on("error", closeModel);
  session.modelConn.on("close", closeModel);
}

function handleModelMessage(data: RawData) {
  const event = parseMessage(data);
  if (!event) return;

  jsonSend(session.frontendConn, event);
  // console.log("event", event)
  if (event.item?.type === "function_call") {
    console.log("function_call event:", event);
  }
  switch (event.type) {
    case "response.content_part.done":
      console.log("response.content_part.done", event);
      break;
    case "input_audio_buffer.speech_started":
      // console.log("input_audio_buffer.speech_started", event);
      handleTruncation();
      break;

    case "response.audio.delta":
      // console.log("response.audio.delta", event);
      if (session.twilioConn && session.streamSid) {
        if (session.responseStartTimestamp === undefined) {
          session.responseStartTimestamp = session.latestMediaTimestamp || 0;
        }
        if (event.item_id) session.lastAssistantItem = event.item_id;

        jsonSend(session.twilioConn, {
          event: "media",
          streamSid: session.streamSid,
          media: { payload: event.delta },
        });

        jsonSend(session.twilioConn, {
          event: "mark",
          streamSid: session.streamSid,
        });
      }
      break;
    case "response.output_item.added": {
      console.log("response.output_item.added!", event);
      break;
    }


    case "response.output_item.done": {
      console.log("response.output_item.done!", event);
      const { item } = event;
      


      if (item.type === "function_call") {
        console.log("response.output_item.done", item);
        handleFunctionCall(item)
          .then((output) => {
            if (session.modelConn) {
              jsonSend(session.modelConn, {
                type: "conversation.item.create",
                item: {
                  type: "function_call_output",
                  call_id: item.call_id,
                  output: JSON.stringify(output),
                },
              });
              jsonSend(session.modelConn, { type: "response.create" });

              if (session.twilioConn && session.streamSid) {
                const base64Audio = "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3XRQ9/5ZEvJ1Vdt7/2/uan48etj1jXBV/aZzu6KwLQWKBmqauK9Spl1OUpkoCmKpqB0RXWiUTqLlxKJ1iUJQlOwmJ6ciSJJNdOjIyMiUIQlCUJR6YiSJJNMVpye2YkS1qqiKPc2SIBCdmWOSo4GIkZbXnycDBWnEtkqSJGWSo4lMwSsAq3BITJpGSJFE0AqBk5cicUAk0WNgGSbQUJmW0iaASKMmkSMkl5VAwUSOI4aRAIBVTu1PJRIkDEY2ZIoyzEgrSKM+pI54KC5BQV8Z/gv/C+Ed1gKf/BvjFUxBTUUzLjEwMFVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVf/7ksTug5j+AP4MJNfK9bSeiYYZeVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVU=";
                
                jsonSend(session.twilioConn, {
                  event: "media",
                  streamSid: session.streamSid,
                  media: { payload: base64Audio },
                });

                jsonSend(session.twilioConn, {
                  event: "mark",
                  streamSid: session.streamSid,
                });
              }
            }
          })
          .catch((err) => {
            console.error("Error handling function call:", err);
          });
      }
      break;
    }

    // case "input_audio_buffer.speech_stopped": {
    //   console.log("input_audio_buffer.speech_stopped");
    //   if (session.modelConn) {
    //     console.log("connected to model");
    //     // Add the assistant message to the conversation
    //     jsonSend(session.modelConn, {
    //       type: "conversation.item.create",
    //       item: {
    //         type: "message",
    //         role: "assistant",
    //         content: [
    //           {
    //             type: "text",
    //             text: "Okay."
    //           }
    //         ]
    //       }
    //     });
      
    //     // Trigger the model to generate a response
    //     jsonSend(session.modelConn, {
    //       type: "response.create"
    //     });
    //   }
    //   break;
    // }
  }
}

function handleTruncation() {
  if (
    !session.lastAssistantItem ||
    session.responseStartTimestamp === undefined
  )
    return;

  const elapsedMs =
    (session.latestMediaTimestamp || 0) - (session.responseStartTimestamp || 0);
  const audio_end_ms = elapsedMs > 0 ? elapsedMs : 0;

  if (isOpen(session.modelConn)) {
    jsonSend(session.modelConn, {
      type: "conversation.item.truncate",
      item_id: session.lastAssistantItem,
      content_index: 0,
      audio_end_ms,
    });
  }

  if (session.twilioConn && session.streamSid) {
    jsonSend(session.twilioConn, {
      event: "clear",
      streamSid: session.streamSid,
    });
  }

  session.lastAssistantItem = undefined;
  session.responseStartTimestamp = undefined;
}

export function closeModel() {
  cleanupConnection(session.modelConn);
  session.modelConn = undefined;
  if (!session.twilioConn && !session.frontendConn) session = {};
}

export function closeAllConnections() {
  if (session.twilioConn) {
    session.twilioConn.close();
    session.twilioConn = undefined;
  }
  if (session.modelConn) {
    session.modelConn.close();
    session.modelConn = undefined;
  }
  if (session.frontendConn) {
    session.frontendConn.close();
    session.frontendConn = undefined;
  }
  if (session.deepgramConn) {
    session.deepgramConn.finish();
    session.deepgramConn = undefined;
  }
  // if (session.recorder) {
  //   session.recorder.finalize()
  //     .then(outputPath => {
  //       console.log(`Recording saved for call ${session.streamSid}: ${outputPath}`);
  //     })
  //     .catch(err => {
  //       console.error(`Error finalizing recording for call ${session.streamSid}:`, err);
  //     });
  //   session.recorder = undefined;
  // }
  // session.streamSid = undefined;
  session.lastAssistantItem = undefined;
  session.responseStartTimestamp = undefined;
  session.latestMediaTimestamp = undefined;
  session.saved_config = undefined;
}

function cleanupConnection(ws?: WebSocket) {
  if (isOpen(ws)) ws.close();
}

function parseMessage(data: RawData): any {
  try {
    const msg = JSON.parse(data.toString());
    return msg;
  } catch {
    return null;
  }
}

function jsonSend(ws: WebSocket | undefined, obj: unknown) {
  if (!isOpen(ws)) return;
  ws.send(JSON.stringify(obj));
}

function isOpen(ws?: WebSocket): ws is WebSocket {
  return !!ws && ws.readyState === WebSocket.OPEN;
}
