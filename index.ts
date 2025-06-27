/* eslint-disable no-console */
import PubSubApiClient from 'salesforce-pubsub-api-client';
import { ConvexHttpClient } from 'convex/browser';

// --- Configuration ---
const {
  SF_LOGIN_URL,
  SF_CLIENT_ID,
  SF_CLIENT_SECRET,
  CONVEX_URL,
  CONVEX_WEBHOOK_SECRET,
} = process.env;

// List of CDC topics to subscribe to
const TOPICS = [
  '/data/Claim__cChangeEvent',
  '/data/AccountChangeEvent',
  '/data/CaseChangeEvent',
];

if (!CONVEX_URL || !CONVEX_WEBHOOK_SECRET) {
  throw new Error('CONVEX_URL and CONVEX_WEBHOOK_SECRET must be set in environment variables.');
}

// --- Main Application Logic ---
async function run() {
  try {
    console.log('Starting Salesforce Pub/Sub listener...');

    // 1. Instantiate the Convex HTTP Client
    const convex = new ConvexHttpClient(CONVEX_URL);
    
    // 2. Build and connect the Pub/Sub API client
    const client = new PubSubApiClient({
      authType: 'oauth-client-credentials',
      loginUrl: SF_LOGIN_URL,
      clientId: SF_CLIENT_ID,
      clientSecret: SF_CLIENT_SECRET,
    });
    await client.connect();
    console.log('Successfully connected to Salesforce Pub/Sub API.');

    // 3. Define the callback for handling incoming events
    const subscribeCallback = async (subscription, callbackType, data) => {
      switch (callbackType) {
        case 'event':
          console.log(`Event received for ${subscription.topicName} with replayId: ${data.replayId}`);
          try {
            // Forward the decoded event to our Convex HTTP Action
            await convex.action('http:receiveCdcEvent', {
              webhookSecret: CONVEX_WEBHOOK_SECRET,
              topicName: subscription.topicName,
              event: data,
            });
            console.log(`Successfully forwarded event ${data.replayId} to Convex.`);
          } catch (error) {
            console.error(`Failed to forward event ${data.replayId} to Convex:`, error);
            // Implement retry logic or dead-letter queue as needed
          }
          break;
        case 'error':
            console.error('gRPC stream error:', JSON.stringify(data));
            break;
        case 'end':
            console.log('gRPC stream ended. The service will likely restart.');
            break;
        default:
            // You can also handle 'lastEvent', 'grpcStatus', etc.
            break;
      }
    };

    // 4. Subscribe to each topic
    for (const topicName of TOPICS) {
      // We subscribe for an indefinite number of events
      await client.subscribe(topicName, subscribeCallback);
      console.log(`Subscribed to topic: ${topicName}`);
    }

  } catch (error) {
    console.error('An unrecoverable error occurred:', error);
    // Railway will typically restart the service on a crash
    process.exit(1);
  }
}

run();