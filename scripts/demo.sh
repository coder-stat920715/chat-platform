#!/usr/bin/env bash
# Creates a conversation with users 1,2,3 and a 2000-member style channel flag example.
set -euo pipefail
GW=${GW:-http://localhost:8080}
curl -s -X POST "$GW/dev/conversations" -H 'Content-Type: application/json' \
  -d '{"conversationId":100,"members":[1,2,3],"channelMode":false}'
curl -s -X POST "$GW/dev/conversations" -H 'Content-Type: application/json' \
  -d '{"conversationId":200,"members":[1,2,3],"channelMode":true}'
echo "created conversation 100 (group, fan-out on write) and 200 (channel, fan-out on read)"
echo
echo "Open two terminals and connect (websocat: https://github.com/vi/websocat):"
echo "  websocat 'ws://localhost:8080/ws/v1/chat?token=dev-2&deviceId=phone'"
echo "  websocat 'ws://localhost:8080/ws/v1/chat?token=dev-1&deviceId=laptop'"
echo
echo "Then send from user 1 (replace the UUID each time):"
echo '  {"type":"SEND","correlationId":"c1","payload":{"clientMsgId":"11111111-1111-1111-1111-111111111111","conversationId":100,"type":"TEXT","body":"hello"}}'
echo "Channel mode needs a subscription first:"
echo '  {"type":"SUBSCRIBE","payload":{"conversationId":200}}'
echo "History:"
echo "  curl 'http://localhost:8082/api/conversations/100/messages?limit=20'"
