import { AgentWharfClient } from '../../client-ts/dist/src/index.js'

const form = document.querySelector('#connect-form')
const messageForm = document.querySelector('#message-form')
const hubUrl = document.querySelector('#hub-url')
const token = document.querySelector('#token')
const sessionId = document.querySelector('#session-id')
const statusText = document.querySelector('#status')
const lastSeq = document.querySelector('#last-seq')
const events = document.querySelector('#events')
const message = document.querySelector('#message')
const send = document.querySelector('#send')
const disconnect = document.querySelector('#disconnect')

let client = null

form.addEventListener('submit', async (event) => {
  event.preventDefault()
  resetEvents()
  client?.close()
  client = new AgentWharfClient({
    url: hubUrl.value,
    token: token.value,
    sessions: [{ sessionId: sessionId.value }],
  })
  client.onEvent((frame) => {
    lastSeq.textContent = String(client.lastSeq(frame.session_id))
    appendEvent(frame)
  })
  client.onError((error) => {
    statusText.textContent = 'Error'
    appendEvent(error)
  })

  try {
    statusText.textContent = 'Connecting'
    const ack = await client.connect()
    statusText.textContent = ack.sessions[0]?.state ?? 'Connected'
    lastSeq.textContent = String(ack.sessions[0]?.latest_seq ?? 0)
  } catch (error) {
    statusText.textContent = 'Disconnected'
    appendEvent(error)
  }
})

messageForm.addEventListener('submit', async (event) => {
  event.preventDefault()
  if (client === null || message.value.trim() === '') {
    return
  }
  const text = message.value
  message.value = ''
  send.disabled = true
  try {
    const ack = await client.sendMessage(sessionId.value, [{ kind: 'text', text }])
    statusText.textContent = ack.status
  } catch (error) {
    statusText.textContent = 'Send failed'
    appendEvent(error)
  } finally {
    send.disabled = false
  }
})

disconnect.addEventListener('click', () => {
  client?.close()
  client = null
  statusText.textContent = 'Disconnected'
})

function resetEvents() {
  events.replaceChildren()
  lastSeq.textContent = '0'
}

function appendEvent(frame) {
  const row = document.createElement('article')
  row.className = 'event'

  const title = document.createElement('strong')
  title.textContent = frame.frame ?? frame.code ?? frame.name ?? 'error'

  const meta = document.createElement('span')
  meta.textContent = frame.type ?? frame.message ?? ''

  const body = document.createElement('pre')
  body.textContent = JSON.stringify(frame, null, 2)

  row.append(title, meta, body)
  events.append(row)
  events.scrollTop = events.scrollHeight
}
