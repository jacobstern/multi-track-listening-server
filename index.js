const Queue = require('firebase-queue')
const admin = require('firebase-admin')
const dotenv = require('dotenv')
const gcloud = require('google-cloud')
const lame = require('lame')
const progressStream = require('progress-stream')
const shortid = require('shortid')
const wav = require('wav')

dotenv.config()

const env = process.env.NODE_ENV || 'development'

const projectId = 'multi-track-listening'
const privateKey = process.env.FIREBASE_PRIVATE_KEY
const clientEmail = process.env.FIREBASE_CLIENT_EMAIL
const storage = gcloud.storage({
  projectId,
  credentials: {
    private_key: privateKey,
    client_email: clientEmail
  }
})
const bucket = storage.bucket('multi-track-listening.appspot.com')

admin.initializeApp({
  credential: admin.credential.cert({
    projectId,
    privateKey,
    clientEmail
  }),
  databaseURL: 'https://multi-track-listening.firebaseio.com'
})
const database = admin.database()

const options = {
  numWorkers: 50,
  suppressStack: false
}

const mixesRef = database.ref('mixes')
const mixesAliasRef = database.ref('mixesAlias')
const queueRef = database.ref('queue')
const queue = new Queue(queueRef, options, (data, progress, resolve, reject) => {
  const { uid, song1Name, song2Name } = data
  const inputFile = bucket.file(`raw/${uid}`)
  const input = inputFile.createReadStream()
  const serverId = shortid.generate()
  const outputFile = bucket.file('public/' + serverId)
  const output = outputFile.createWriteStream({ metadata: { contentType: 'audio/mp3' } })
  const reader = new wav.Reader()

  input
    .pipe(reader)
    .on('format', format => {
      const encoder = new lame.Encoder(format)
        .on('error', reject)
      const progressInstance = progressStream({
        length: reader.chunkSize,
        time: 200 // Milliseconds between updates
      })
        .on('progress', val => progress(val.percentage))

      reader
        .pipe(progressInstance)
        .pipe(encoder)
        .pipe(output)
        .on('finish', () => {
          outputFile.makePublic()
            .then(() =>
              inputFile.delete()
                .then(() => {
                  const url = `https://storage.googleapis.com/multi-track-listening.appspot.com/public/${serverId}`

                  return mixesRef.child(serverId).set({
                    url,
                    song1Name,
                    song2Name
                  })
                    .then(() => mixesAliasRef.set({ [uid]: serverId })
                      .then(() => resolve())
                    )
                })
            )
            .catch(reject)
        })
        .on('error', reject)
    })
    .on('error', reject)
})

process.on('SIGINT', () => {
  console.info('Starting queue shutdown')

  queue.shutdown().then(() => {
    console.info('Finished queue shutdown')
    process.exit(0)
  })
})
