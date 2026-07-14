import { Stack } from "./stack.js";
/** One FSP service with an automatically matching internal TCP listener. */
export class FipsTcpEndpoint {
    endpoint;
    fspServicePort;
    stack;
    unregister;
    operation = Promise.resolve();
    constructor(endpoint, fspServicePort, config = {}, isnSeed = 1n) {
        this.endpoint = endpoint;
        this.fspServicePort = fspServicePort;
        checkFspServicePort(fspServicePort);
        this.stack = new Stack(config, isnSeed);
        this.stack.listen(fspServicePort);
        this.unregister = endpoint.registerService(fspServicePort, (context) => this.enqueue(async () => {
            this.stack.input(context.src, context.payload, Date.now());
            await this.flush();
        }));
    }
    async accept() {
        return this.enqueue(() => this.stack.accept(this.fspServicePort));
    }
    async connect(peer, nowMs = Date.now()) {
        return this.enqueue(async () => {
            const id = this.stack.connect(peer, this.fspServicePort, nowMs);
            await this.flush();
            return id;
        });
    }
    async write(id, bytes, nowMs = Date.now()) {
        return this.enqueue(async () => {
            const accepted = this.stack.write(id, bytes, nowMs);
            await this.flush();
            return accepted;
        });
    }
    async read(id, max, nowMs = Date.now()) {
        return this.enqueue(async () => {
            const bytes = this.stack.read(id, max, nowMs);
            await this.flush();
            return bytes;
        });
    }
    async close(id, nowMs = Date.now()) {
        await this.enqueue(async () => {
            this.stack.close(id, nowMs);
            await this.flush();
        });
    }
    async poll(nowMs = Date.now()) {
        await this.enqueue(async () => {
            this.stack.poll(nowMs);
            await this.flush();
        });
    }
    async state(id) {
        return this.enqueue(() => this.stack.state(id));
    }
    async isReadClosed(id) {
        return this.enqueue(() => this.stack.isReadClosed(id));
    }
    /** Return the authenticated FIPS identity bound to this stream. */
    async peer(id) {
        return this.enqueue(() => this.stack.peer(id));
    }
    /** Return the stream's internal `[local, remote]` TCP ports. */
    async ports(id) {
        return this.enqueue(() => this.stack.ports(id));
    }
    async dispose() {
        await this.operation;
        this.unregister();
    }
    async flush() {
        for (const outbound of this.stack.drainOutbound()) {
            await this.endpoint.sendDatagram({
                dst: outbound.peer,
                srcPort: this.fspServicePort,
                dstPort: this.fspServicePort,
                payload: outbound.bytes,
            });
        }
    }
    enqueue(work) {
        const result = this.operation.then(work, work);
        this.operation = result.then(() => undefined, () => undefined);
        return result;
    }
}
function checkFspServicePort(port) {
    if (!Number.isInteger(port) || port <= 0 || port > 0xffff) {
        throw new Error("FIPS service port must be an integer between 1 and 65535");
    }
}
