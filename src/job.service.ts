import {
  FactoryProvider,
  Inject,
  Injectable,
  SetMetadata,
} from "@nestjs/common";
import { PgBoss } from "pg-boss";
import type * as PgBossTypes from "pg-boss";
import { HandlerMetadata } from "./interfaces/handler-metadata.interface";
import { PG_BOSS_JOB_METADATA } from "./pg-boss.constants";
import { getJobToken } from "./utils";

@Injectable()
export class JobService<JobData extends object> {
  constructor(
    private readonly name: string,
    private readonly pgBoss: PgBoss,
  ) {}

  async send(
    data: JobData,
    options: PgBossTypes.SendOptions,
  ): Promise<string | null> {
    return this.pgBoss.send(this.name, data, options);
  }

  async sendAfter(
    data: JobData,
    options: PgBossTypes.SendOptions,
    date: Date | string | number,
  ): Promise<string | null> {
    // sendAfter has three overloads for all date variants we accept
    return this.pgBoss.sendAfter(this.name, data, options, date as any);
  }

  async sendOnce(
    data: JobData,
    options: PgBossTypes.SendOptions,
    key: string,
  ): Promise<string | null> {
    return this.pgBoss.send(this.name, data, { ...options, singletonKey: key });
  }

  async sendSingleton(
    data: JobData,
    options: PgBossTypes.SendOptions,
  ): Promise<string | null> {
    return this.pgBoss.send(this.name, data, { ...options, singletonKey: this.name });
  }

  async sendThrottled(
    data: JobData,
    options: PgBossTypes.SendOptions,
    seconds: number,
    key?: string,
  ): Promise<string | null> {
    if (key != undefined) {
      return this.pgBoss.sendThrottled(this.name, data, options, seconds, key);
    }
    return this.pgBoss.sendThrottled(this.name, data, options, seconds);
  }

  async sendDebounced(
    data: JobData,
    options: PgBossTypes.SendOptions,
    seconds: number,
    key?: string,
  ): Promise<string | null> {
    if (key != undefined) {
      return this.pgBoss.sendDebounced(this.name, data, options, seconds, key);
    }
    return this.pgBoss.sendDebounced(this.name, data, options, seconds);
  }

  async insert(jobs: Omit<PgBossTypes.JobInsert, "name">[]): Promise<any> {
    return this.pgBoss.insert(this.name, jobs);
  }

  async schedule(cron: string, data: JobData, options: PgBossTypes.ScheduleOptions) {
    return this.pgBoss.schedule(this.name, cron, data, options);
  }

  async unschedule() {
    this.pgBoss.unschedule(this.name);
  }
}

export interface WorkHandler<ReqData> {
  (job?: PgBossTypes.Job<ReqData>): Promise<void>;
}

export interface WorkHandlerBatch<ReqData> {
  (jobs?: PgBossTypes.Job<ReqData>[]): Promise<void>;
}

interface MethodDecorator<PropertyType> {
  <Class>(
    target: Class,
    propertyKey: string | symbol,
    descriptor: TypedPropertyDescriptor<PropertyType>,
  ): TypedPropertyDescriptor<PropertyType>;
}

interface HandleDecorator<JobData extends object> {
  <Options extends PgBossTypes.WorkOptions>(
    options?: Options,
  ): MethodDecorator<
    Options extends { batchSize: number }
      ? WorkHandlerBatch<JobData>
      : WorkHandler<JobData>
  >;
}

export interface Job<JobData extends object = any> {
  ServiceProvider: FactoryProvider<JobService<JobData>>;
  Inject: () => ParameterDecorator;
  Handle: HandleDecorator<JobData>;
}

export const createJob = <JobData extends object>(
  name: string,
): Job<JobData> => {
  const token = getJobToken(name);

  return {
    ServiceProvider: {
      provide: token,
      useFactory: (pgBoss: PgBoss) => new JobService<JobData>(name, pgBoss),
      inject: [PgBoss],
    },
    Inject: () => Inject(token),
    Handle: (options: PgBossTypes.WorkOptions = {}) =>
      SetMetadata<string, HandlerMetadata>(PG_BOSS_JOB_METADATA, {
        token,
        jobName: name,
        workOptions: options,
      }),
  };
};
