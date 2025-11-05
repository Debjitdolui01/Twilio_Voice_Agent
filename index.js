import Fastify from 'fastify';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';
import twilio from "twilio";
import { MongoClient, ObjectId } from 'mongodb';
import * as chrono from "chrono-node";

// Load environment variables from .env file
dotenv.config();

// Retrieve the OpenAI API key from environment variables.
const { OPENAI_API_KEY } = process.env;

// Retrieve Twilio credentials from environment variables
const { TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER, NGROK_URL, MONGODB_URI } = process.env;
const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

if (!OPENAI_API_KEY) {
    console.error('Missing OpenAI API key. Please set it in the .env file.');
    process.exit(1);
}

// Initialize Fastify
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

// Store active calls temporarily to track phone numbers
const activeCalls = new Map();

// Constants
const SYSTEM_MESSAGE = `

At the start of a conversation (first message only), automatically greet with:
"Hello, I am Basanta from Workmates Core2Cloud, AWS Premier Tier Partner. How can I assist with your AWS needs today?"
After that first message, no more automatic greeting in later replies — just continue normal conversation as Basanta, the AWS Solutions Architect.

You are Basanta, a senior AWS Solutions Architect at Workmates Core2Cloud (CloudWorkmates),an AWS Premier Tier Services Partner. 
Workmates Core2Cloud is the fastest-growing AWS Premier Consulting Partner in India, specializing in cloud managed services, AWS migrations, DevOps, cost optimization, security, and generative AI solutions. 
You provide authoritative, production-grade AWS guidance: architectures, services, cost models, security, reliability, performance, and operations—grounded in AWS best practices and the AWS Well-Architected Framework.

Language:
**Always start conversation in "English language", if user want to speak in other language then only respond in that language. **

## APPOINTMENT BOOKING FLOW:
When user wants to book an appointment, follow this EXACT sequence:

STEP 1: Ask for full name
      -Say: "May I have your full name please?"

STEP 2: CONFIRM the name
      -Say: "So your name is [Name], is that correct?"
      -WAIT for user confirmation before proceeding
      -If incorrect, ask: "Could you please repeat your full name or spell it out?"

STEP 3: Ask for purpose
      -Say: "What is the purpose or reason for this appointment?"

STEP 4: Ask for preferred date
      -Say: "What is your preferred appointment date? Please provide a future date."

      Always interpret natural language date expressions using chrono.
            Examples: "tomorrow", "day after tomorrow", "next Friday".
            If date is less than or equal to today, interpret it as the next occurrence in the future.
            Never reject "tomorrow" or "day after tomorrow".

STEP 5: Summarize and confirm
 After collecting all details, say:
"Let me confirm your appointment details:
Name: [Name]
Date: [Date]
Purpose: [Purpose]
Should I proceed with booking this appointment?"

CRITICAL RULE:
NEVER call the book_appointment function until ALL THREE fields (name, date, purpose) are collected AND confirmed by the user in Step 5.

STEP 6: Book appointment
      -Only after user confirms in Step 5, use the 'book_appointment' function to book the appointment.
Do not deviate from this sequence. Always complete each step before moving to the next.

## Company Overview
- Workmates Core2Cloud (CloudWorkmates) is a cloud managed services company focused on AWS services.
- Recognized as the fastest-growing AWS Premier Consulting Partner in India.
- Website: https://cloudworkmates.com/

## Mission, Vision & Values
- Mission: Empower businesses to achieve their full potential through innovation and reliable cloud solutions.
- Vision: Become the leading cloud services provider, known for exceptional customer service, technical expertise, and commitment to excellence.
- Values: Customer Focus, Innovation, Accountability, Teamwork, Integrity.

## Services Offered
- Cloud Consulting, Cloud Migration, Cost Optimization, DevOps, Deployment, Managed Services, Well-Architected Reviews, Generative AI Solutions.

## Solutions
- Microsoft on AWS, Tally on AWS Cloud, AWS Media Solutions, SAP on AWS, FlickOtt with Workmates, Empowering SMBs with AWS Solutions, Accelerate with Workmates and AWS.

## Cybersecurity Services
- AWS Managed Security Services, Red/Blue Teaming Services, Cybersecurity Managed Services, Cyber Range-based Simulation Services.
- Security with AWS WAF, IAM, Threat Detection, Compliance & Data Privacy.

## Cloud Deployment Services
- Architecture Consulting: Designing cost-optimized, scalable cloud architectures.
- Data Migration to Cloud: Using AWS Snowball, Lambda, and S3 for efficient migrations.
- Hosting Solutions: High-performance AWS hosting for enterprise workloads.

## Case Studies & Success Stories
- ULURN: Implemented AWS Glue ETL processes for educational content streaming.
- Annapurna Finance: Cloud adoption for operational efficiency.
- SMBs: Strategic cloud adoption roadmap tailored for SMBs.
- Security Transformation: Strengthened cybersecurity for IPL franchise & Celex Technologies.

## Client Testimonials
- CIOs, IT Directors, and Developers praised Workmates for rapid support, AWS expertise, seamless migrations, real-time problem solving, and 24x7 support.

---

## Mission (Your Role as Basanta)
- Diagnose needs, propose AWS-first solutions, and explain trade-offs clearly.
- Map business goals to AWS reference architectures and managed services.
- Keep responses practical, implementation-focused, and step-by-step when useful.

## Scope & Guardrails:
- **Company Information Beyond Provided Scope**: Do not answer any questions about Workmates Core2Cloud that are not explicitly mentioned in this system message
- **Other Cloud Providers**: Do not provide detailed information about Azure, GCP, or other cloud platforms beyond high-level comparisons
- **Competitor Information**: Do not discuss or compare with other AWS partners or IT companies
- **Internal Company Details**: Do not speculate about internal processes, team structure, or unpublished information
- **Financial Information**: Do not discuss revenue, pricing models, or financial performance beyond published AWS pricing
- **Future Roadmaps**: Do not speculate about upcoming services or company plans

**For company information questions beyond provided data:**
"Please visit our official website at https://cloudworkmates.com or contact our sales team directly."

**For other cloud provider questions:**
"My expertise is focused on AWS solutions. I can help you with AWS migration paths or equivalent AWS services for your needs."

**For competitor or other company inquiries:**
"I'm designed to provide AWS technical guidance through Workmates Core2Cloud. I don't have information about other companies in the ecosystem."

**For unrelated topics:**
"I'm here to help with AWS architecture and cloud solutions. How can I assist with your AWS requirements today?"

## Handling Non-AWS Requests
- If the request is solely about non-AWS clouds: provide a concise comparison and immediately re-center on AWS equivalents.
- If user insists, give only high-level comparisons, then recommend AWS alternatives.

## Style
- Conversational, concise, structured.
- Prefer managed services over self-managed components.
- Reference Well-Architected pillars: Security, Reliability, Performance Efficiency, Cost Optimization, Sustainability, Operational Excellence.
- Provide diagrams-in-words when helpful.

## Tool Use
- Use tools (AWS queries, web, emails) only when they add concrete value.
- Summarize results and map them to AWS recommendations.

## Safety & Compliance
- Do not share AWS internal/partner-only information.
- No PII retention beyond transient use.

## Output Expectations
- Lead with recommendation → rationale → next steps (services, configs, IaC hints).
- Use AWS service mappings (e.g., "GKE → Amazon EKS", "BigQuery → Amazon Redshift/S3+Athena").

**About the voice of the responses:**
Voice: Clear, authoritative, and composed, projecting confidence and professionalism.
Tone: Neutral and informative, maintaining a balance between formality and approachability.
Punctuation: Structured with commas and pauses for clarity, ensuring information is digestible and well-paced.
Delivery: Steady and measured, with slight emphasis on key figures and deadlines to highlight critical points.

**Don't hallucinate or make up answers. If you don't understand the question properly, ask for clarification. **
`;

const VOICE = 'echo';
const TEMPERATURE = 0.8;
const PORT = process.env.PORT;

// IST Timezone utilities
const IST_OFFSET = 5.5 * 60 * 60 * 1000; // IST is UTC+5:30

// Helper function to get current IST date
function getCurrentISTDate() {
    const now = new Date();
    return new Date(now.getTime() + IST_OFFSET);
}

// Helper function to convert date to IST
function toIST(date) {
    return new Date(date.getTime() + IST_OFFSET);
}

// Helper function to format date in IST for display
function formatISTDate(date) {
    const istDate = toIST(date);
    return istDate.toLocaleDateString('en-IN', {
        timeZone: 'Asia/Kolkata',
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric'
    });
}

// Helper function to get IST date in YYYY-MM-DD format
function getISTDateString(date) {
    const istDate = toIST(date);
    const year = istDate.getFullYear();
    const month = String(istDate.getMonth() + 1).padStart(2, '0');
    const day = String(istDate.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

// MongoDB connection setup
let mongoClient;
let appointmentsCollection;

// Initialize MongoDB connection
async function initializeMongoDB() {
    if (!MONGODB_URI) {
        console.warn('MongoDB URI not found. Appointment storage will be disabled.');
        return;
    }

    try {
        mongoClient = new MongoClient(MONGODB_URI);
        await mongoClient.connect();
        const db = mongoClient.db('workmates_appointments');
        appointmentsCollection = db.collection('appointments');
        console.log('MongoDB connected successfully');
    } catch (error) {
        console.error('Failed to connect to MongoDB:', error);
    }
}

// Store appointment in MongoDB
async function storeAppointmentInMongoDB(appointmentData) {
    if (!appointmentsCollection) {
        console.warn('MongoDB not configured. Appointment not stored.');
        return { success: false, message: 'Database not configured' };
    }

    try {
        // Validate date is in the future (using IST)
        const appointmentDate = new Date(appointmentData.date + 'T00:00:00+05:30'); // Treat as IST date
        const todayIST = getCurrentISTDate();
        todayIST.setHours(0, 0, 0, 0);
        
        console.log('IST Date Validation:');
        console.log('Appointment Date (IST):', appointmentDate.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
        console.log('Today IST:', todayIST.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
        console.log('Is future date?', appointmentDate >= todayIST);

        if (appointmentDate < todayIST) {
            return { 
                success: false, 
                message: 'Appointment date must be in the future' 
            };
        }

        const appointmentRecord = {
            ...appointmentData,
            status: 'pending',
            createdAt: new Date(),
            updatedAt: new Date(),
            timezone: 'IST'
        };

        const result = await appointmentsCollection.insertOne(appointmentRecord);
        
        console.log('Appointment stored in MongoDB with ID:', result.insertedId);
        return { 
            success: true, 
            message: 'Appointment stored successfully',
            appointmentId: result.insertedId 
        };
    } catch (error) {
        console.error('Error storing appointment in MongoDB:', error);
        return { 
            success: false, 
            message: 'Failed to store appointment in database' 
        };
    }
}

// Send SMS confirmation
async function sendSMSConfirmation(phoneNumber, appointmentDetails) {
    if (!phoneNumber || !TWILIO_NUMBER) {
        console.log('SMS not sent: missing phone number or Twilio number');
        return false;
    }

    try {
        // Format date for display in IST
        const appointmentDate = new Date(appointmentDetails.date + 'T00:00:00+05:30');
        const formattedDate = formatISTDate(appointmentDate);

        const message = `Hello ${appointmentDetails.name}! Your appointment with Workmates Core2Cloud is pending confirmation. 
Date: ${formattedDate}
Purpose: ${appointmentDetails.purpose}
We'll contact you shortly to confirm. Thank you!`;

        await twilioClient.messages.create({
            body: message,
            from: TWILIO_NUMBER,
            to: phoneNumber
        });
        
        console.log('SMS confirmation sent to:', phoneNumber);
        return true;
    } catch (error) {
        console.error('Failed to send SMS:', error);
        return false;
    }
}

// Show AI response elapsed timing calculations
const SHOW_TIMING_MATH = false;

// Root Route
fastify.get('/', async (request, reply) => {
    reply.send({ message: 'Twilio Media Stream Server is running!' });
});

// Make call route
fastify.post('/make_call', async (request, reply) => {
    const { to } = request.body;
    if (!to) return reply.code(400).send({ error: "Missing 'to' number" });

    try {
        const call = await twilioClient.calls.create({
            from: TWILIO_NUMBER,
            to: to,
            url: `${NGROK_URL}/incoming-call`,
        });
        return reply.send(call);
    } catch (error) {
        console.error("Error making call:", error);
        return reply.code(500).send({ error: error.message });
    }
});

// Route for Twilio to handle incoming calls
fastify.all('/incoming-call', async (request, reply) => {
    const fromNumber = request.body.From;
    const callSid = request.body.CallSid;
    
    console.log('Incoming call from:', fromNumber, 'Call SID:', callSid);
    console.log('Current IST time:', getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
    
    // Store call information in memory
    activeCalls.set(callSid, {
        phoneNumber: fromNumber,
        callSid: callSid,
        timestamp: Date.now(),
        istTime: getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })
    });
    
    // Clean up old entries (older than 1 hour)
    const oneHourAgo = Date.now() - 3600000;
    for (let [key, value] of activeCalls.entries()) {
        if (value.timestamp < oneHourAgo) {
            activeCalls.delete(key);
        }
    }
    
    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Connect>
        <Stream url="wss://${request.headers.host}/media-stream" />
    </Connect>
</Response>`;

    reply.type('text/xml').send(twimlResponse);
});

// WebSocket route for media-stream
fastify.register(async (fastify) => {
    fastify.get('/media-stream', { websocket: true }, (connection, req) => {
        console.log('Client connected at IST:', getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));

        // Connection-specific state
        let streamSid = null;
        let latestMediaTimestamp = 0;
        let lastAssistantItem = null;
        let markQueue = [];
        let responseStartTimestampTwilio = null;
        let callerPhoneNumber = null;
        let currentCallSid = null;

        const openAiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=gpt-4o-mini-realtime-preview-2024-12-17&temperature=${TEMPERATURE}`, {
            headers: {
                Authorization: `Bearer ${OPENAI_API_KEY}`,
            }
        });

        let appointmentState = {
            name: null,
            purpose: null,
            date: null,
            confirmed: false
        };

        // Define tools for function calling
        const TOOLS = {
            book_appointment: async ({ name, date, purpose }) => {
                console.log("=== APPOINTMENT BOOKING STARTED ===");
                console.log("Current IST:", getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
                console.log("Input details:", { name, date, purpose, phoneNumber: callerPhoneNumber });

                // Validate required fields
                if (!name || !date || !purpose) {
                    return "Missing required appointment details. Please provide name, date, and purpose.";
                }

                // Parse natural language date using chrono in IST context
                const nowIST = getCurrentISTDate();
                let appointmentDate = chrono.parseDate(date, nowIST, { forwardDate: true });
                
                if (!appointmentDate) {
                    return "I couldn't understand the date. Please say something like 'tomorrow' or '26 October 2025'.";
                }

                // Convert to IST for accurate comparison
                const appointmentDateIST = toIST(appointmentDate);
                appointmentDateIST.setHours(0, 0, 0, 0);

                const todayIST = getCurrentISTDate();
                todayIST.setHours(0, 0, 0, 0);

                // Debug logging in IST
                console.log('=== IST DATE COMPARISON ===');
                console.log('Input date string:', date);
                console.log('Parsed date (IST):', appointmentDateIST.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
                console.log('Today (IST):', todayIST.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
                console.log('Is future date?', appointmentDateIST > todayIST);

                // Validate it's a future date in IST
                if (appointmentDateIST <= todayIST) {
                    return "Appointment date must be in the future. Please provide a future date like 'tomorrow', 'next Monday', or a specific future date.";
                }

                // Store date in YYYY-MM-DD format (IST)
                date = getISTDateString(appointmentDate);
                console.log('Final stored date (IST):', date);

                // Store appointment in MongoDB
                const appointmentData = {
                    name: name,
                    date: date,
                    purpose: purpose,
                    phoneNumber: callerPhoneNumber,
                    callSid: currentCallSid,
                    source: 'voice_call',
                    timezone: 'IST'
                };

                const storageResult = await storeAppointmentInMongoDB(appointmentData);

                if (storageResult.success) {
                    // Send SMS confirmation
                    if (callerPhoneNumber) {
                        await sendSMSConfirmation(callerPhoneNumber, appointmentData);
                    }

                    const formattedDisplayDate = formatISTDate(appointmentDateIST);
                    return `Appointment successfully booked for ${name}! Date: ${formattedDisplayDate}, Purpose: ${purpose}. Your appointment is pending confirmation. ${callerPhoneNumber ? 'A confirmation message has been sent to your phone.' : 'Please note your appointment details.'}`;
                } else {
                    return `Appointment details recorded for ${name}, but there was an issue storing the appointment in our system. Please contact us directly to confirm.`;
                }
            }
        };

        // Control initial session with OpenAI
        const initializeSession = () => {
            const sessionUpdate = {
                type: 'session.update',
                session: {
                    type: 'realtime',
                    model: "gpt-4o-mini-realtime-preview-2024-12-17",
                    output_modalities: ["audio"],
                    audio: {
                        input: { 
                            format: { type: 'audio/pcmu' },
                            turn_detection: { type: "server_vad" }, 
                            transcription: { model: "whisper-1" } 
                        },
                        output: { format: { type: 'audio/pcmu' }, voice: VOICE },
                    },
                    instructions: SYSTEM_MESSAGE,
                    tools: [
                        {
                            type: "function",
                            name: "book_appointment",
                            description: "Book an appointment after collecting and confirming all details including name, date, and purpose",
                            parameters: {
                                type: "object",
                                properties: {
                                    name: { 
                                        type: "string", 
                                        description: "Full name of the person booking the appointment" 
                                    },
                                    date: { 
                                        type: "string", 
                                        description: "Preferred appointment date in YYYY-MM-DD format (must be future date)" 
                                    },
                                    purpose: { 
                                        type: "string", 
                                        description: "Reason or purpose for the appointment" 
                                    }
                                },
                                required: ["name", "date", "purpose"]
                            }
                        }
                    ],
                    tool_choice: "auto",
                },
            };

            console.log('Sending session update at IST:', getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
            openAiWs.send(JSON.stringify(sessionUpdate));

            // Send initial conversation item to start the conversation
            setTimeout(() => {
                sendInitialConversationItem();
            }, 500);
        };

        // Send initial conversation item if AI talks first
        const sendInitialConversationItem = () => {
            const initialConversationItem = {
                type: 'conversation.item.create',
                item: {
                    type: 'message',
                    role: 'user',
                    content: [
                        {
                            type: 'input_text',
                            text: 'Hello'
                        }
                    ]
                }
            };

            if (SHOW_TIMING_MATH) console.log('Sending initial conversation item');
            openAiWs.send(JSON.stringify(initialConversationItem));
            
            // Create response after a short delay
            setTimeout(() => {
                openAiWs.send(JSON.stringify({ type: 'response.create' }));
            }, 200);
        };

        // Handle interruption when the caller's speech starts
        const handleSpeechStartedEvent = () => {
            if (markQueue.length > 0 && responseStartTimestampTwilio != null) {
                const elapsedTime = latestMediaTimestamp - responseStartTimestampTwilio;
                if (SHOW_TIMING_MATH) console.log(`Calculating elapsed time for truncation: ${latestMediaTimestamp} - ${responseStartTimestampTwilio} = ${elapsedTime}ms`);

                if (lastAssistantItem) {
                    const truncateEvent = {
                        type: 'conversation.item.truncate',
                        item_id: lastAssistantItem,
                        content_index: 0,
                        audio_end_ms: elapsedTime
                    };
                    if (SHOW_TIMING_MATH) console.log('Sending truncation event');
                    openAiWs.send(JSON.stringify(truncateEvent));
                }

                connection.send(JSON.stringify({
                    event: 'clear',
                    streamSid: streamSid
                }));

                // Reset
                markQueue = [];
                lastAssistantItem = null;
                responseStartTimestampTwilio = null;
            }
        };

        // Send mark messages to Media Streams
        const sendMark = (connection, streamSid) => {
            if (streamSid) {
                const markEvent = {
                    event: 'mark',
                    streamSid: streamSid,
                    mark: { name: 'responsePart' }
                };
                connection.send(JSON.stringify(markEvent));
                markQueue.push('responsePart');
            }
        };

        // Open event for OpenAI WebSocket
        openAiWs.on('open', () => {
            console.log('Connected to the OpenAI Realtime API at IST:', getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
            setTimeout(initializeSession, 100);
        });

        // Listen for messages from the OpenAI WebSocket
        openAiWs.on('message', async (data) => {
            try {
                const response = JSON.parse(data);

                if (response.type === 'conversation.item.input_audio_transcription.completed') {
                    console.log('Transcription completed:', response.transcript);
                }

                if (response.type === 'conversation.item.input_audio_transcription.failed') {
                    console.error('Transcription failed:', response.error);
                }

                if (response.type === "response.done") {
                    const outputs = response.response.output;
                    if (outputs.length > 0 && outputs[0].content && outputs[0].content.length > 0) {
                        const transcript = outputs[0].content[0].transcript;
                        console.log('AI RESPONSE:', transcript);
                    }

                    // Handle function calls
                    const functionCall = outputs.find(
                        (output) => output.type === "function_call"
                    );
                    
                    if (functionCall && TOOLS[functionCall.name]) {
                        console.log('Function call detected:', functionCall.name);
                        console.log('Function arguments:', functionCall.arguments);

                        try {
                            let parsedArgs;
                            try {
                                parsedArgs = JSON.parse(functionCall.arguments);
                            } catch (parseError) {
                                console.error('Error parsing function arguments:', parseError);
                                const fixedArgs = functionCall.arguments
                                    .replace(/(\w+):/g, '"$1":')
                                    .replace(/'/g, '"');
                                parsedArgs = JSON.parse(fixedArgs);
                            }

                            const result = await TOOLS[functionCall.name](parsedArgs);

                            const conversationItemCreate = {
                                type: "conversation.item.create",
                                item: {
                                    type: "function_call_output",
                                    call_id: functionCall.call_id,
                                    output: result,
                                },
                            };
                            openAiWs.send(JSON.stringify(conversationItemCreate));

                            setTimeout(() => {
                                openAiWs.send(JSON.stringify({ type: "response.create" }));
                            }, 100);

                        } catch (error) {
                            console.error('Error executing function:', error);
                            
                            const errorOutput = {
                                type: "conversation.item.create",
                                item: {
                                    type: "function_call_output",
                                    call_id: functionCall.call_id,
                                    output: `Error: ${error.message}`,
                                },
                            };
                            openAiWs.send(JSON.stringify(errorOutput));
                            
                            setTimeout(() => {
                                openAiWs.send(JSON.stringify({ type: "response.create" }));
                            }, 100);
                        }
                    }
                }

                if (response.type === 'response.output_audio.delta' && response.delta) {
                    const audioDelta = {
                        event: 'media',
                        streamSid: streamSid,
                        media: { payload: response.delta }
                    };
                    connection.send(JSON.stringify(audioDelta));

                    if (!responseStartTimestampTwilio) {
                        responseStartTimestampTwilio = latestMediaTimestamp;
                        if (SHOW_TIMING_MATH) console.log(`Setting start timestamp for new response: ${responseStartTimestampTwilio}ms`);
                    }

                    if (response.item_id) {
                        lastAssistantItem = response.item_id;
                    }
                    
                    sendMark(connection, streamSid);
                }

                if (response.type === 'input_audio_buffer.speech_started') {
                    handleSpeechStartedEvent();
                }
            } catch (error) {
                console.error('Error processing OpenAI message:', error, 'Raw message:', data);
            }
        });

        // Handle incoming messages from Twilio
        connection.on('message', (message) => {
            try {
                const data = JSON.parse(message);

                switch (data.event) {
                    case 'media':
                        latestMediaTimestamp = data.media.timestamp;
                        if (SHOW_TIMING_MATH) console.log(`Received media message with timestamp: ${latestMediaTimestamp}ms`);
                        if (openAiWs.readyState === WebSocket.OPEN) {
                            const audioAppend = {
                                type: 'input_audio_buffer.append',
                                audio: data.media.payload
                            };
                            openAiWs.send(JSON.stringify(audioAppend));
                        }
                        break;
                    case 'start':
                        streamSid = data.start.streamSid;
                        console.log('Incoming stream has started', streamSid, 'at IST:', getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
                        
                        // Find caller phone number from active calls
                        for (let [callSid, callInfo] of activeCalls.entries()) {
                            if (streamSid.includes(callSid.substring(2)) || callSid.includes(streamSid.substring(2))) {
                                callerPhoneNumber = callInfo.phoneNumber;
                                currentCallSid = callInfo.callSid;
                                console.log('Found matching call info:', { callerPhoneNumber, currentCallSid });
                                break;
                            }
                        }
                        
                        if (!callerPhoneNumber && activeCalls.size > 0) {
                            const firstCall = Array.from(activeCalls.values())[0];
                            callerPhoneNumber = firstCall.phoneNumber;
                            currentCallSid = firstCall.callSid;
                            console.log('Using first active call info:', { callerPhoneNumber, currentCallSid });
                        }

                        responseStartTimestampTwilio = null; 
                        latestMediaTimestamp = 0;
                        break;
                    case 'mark':
                        if (markQueue.length > 0) {
                            markQueue.shift();
                        }
                        break;
                    case 'stop':
                        // Clean up when call ends
                        if (currentCallSid) {
                            activeCalls.delete(currentCallSid);
                            console.log('Cleaned up call:', currentCallSid);
                        }
                        break;
                    default:
                        console.log('Received non-media event:', data.event);
                        break;
                }
            } catch (error) {
                console.error('Error parsing message:', error, 'Message:', message);
            }
        });

        // Handle connection close
        connection.on('close', () => {
            if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close();
            if (currentCallSid) {
                activeCalls.delete(currentCallSid);
                console.log('Cleaned up call on connection close:', currentCallSid);
            }
            console.log('Client disconnected at IST:', getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
        });

        // Handle WebSocket close and errors
        openAiWs.on('close', () => {
            console.log('Disconnected from the OpenAI Realtime API');
        });

        openAiWs.on('error', (error) => {
            console.error('Error in the OpenAI WebSocket:', error);
        });
    });
});

// API Routes for managing appointments
fastify.get('/appointments', async (request, reply) => {
    if (!appointmentsCollection) {
        return reply.code(500).send({ error: 'Database not configured' });
    }

    try {
        const appointments = await appointmentsCollection.find({}).sort({ createdAt: -1 }).toArray();
        
        // Convert dates to IST for display
        const appointmentsWithIST = appointments.map(appointment => ({
            ...appointment,
            displayDate: formatISTDate(new Date(appointment.date + 'T00:00:00+05:30')),
            createdIST: toIST(appointment.createdAt).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })
        }));
        
        return reply.send({ appointments: appointmentsWithIST });
    } catch (error) {
        console.error('Error fetching appointments:', error);
        return reply.code(500).send({ error: 'Failed to fetch appointments' });
    }
});

fastify.get('/appointments/:phoneNumber', async (request, reply) => {
    const { phoneNumber } = request.params;
    
    if (!appointmentsCollection) {
        return reply.code(500).send({ error: 'Database not configured' });
    }

    try {
        const appointments = await appointmentsCollection.find({ 
            phoneNumber: phoneNumber 
        }).sort({ createdAt: -1 }).toArray();
        
        // Convert dates to IST for display
        const appointmentsWithIST = appointments.map(appointment => ({
            ...appointment,
            displayDate: formatISTDate(new Date(appointment.date + 'T00:00:00+05:30')),
            createdIST: toIST(appointment.createdAt).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })
        }));
        
        return reply.send({ 
            phoneNumber,
            appointments: appointmentsWithIST 
        });
    } catch (error) {
        console.error('Error fetching appointments by phone number:', error);
        return reply.code(500).send({ error: 'Failed to fetch appointments' });
    }
});

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('Shutting down gracefully at IST:', getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
    if (mongoClient) {
        await mongoClient.close();
    }
    process.exit(0);
});

// Start server with MongoDB initialization
async function startServer() {
    try {
        await initializeMongoDB();
        
        await fastify.listen({ port: PORT, host: '0.0.0.0' });
        console.log(`Server is listening on port ${PORT} at IST:`, getCurrentISTDate().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
}

startServer();