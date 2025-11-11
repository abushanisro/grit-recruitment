import { wrap, Loaded } from '@mikro-orm/core';
import { InjectRepository } from '@mikro-orm/nestjs';
import { EntityManager, EntityRepository } from '@mikro-orm/postgresql';
import { BadRequestException, Injectable } from '@nestjs/common';
import { getSuccessMsg } from 'src/common/success.ro';
import { Employer } from './entities/employer.entity';
import {
  City,
  Industry,
  Language,
  Skill,
  Subfunction,
} from './entities/info.entity';
import { JobSeeker } from './entities/jobseeker.entity';
import { User } from './entities/user.entity';
import { nestControllerContract, NestRequestShapes } from '@ts-rest/nest';
import { Company } from 'src/job/entities/company.entity';
import { userContract } from 'contract/user/contract';
import { AuthService } from 'src/auth/auth.service';
import { UserRole } from '../../../contract/enum';
import { JobApplication } from 'src/job/entities/jobapplication.entity';
import { EmployerJobSeekerSubscription } from 'src/jobSeekerSubscription/entities/employerJobSeekerSubscription.entity';
import { EmployerPaidJobSubscription } from 'src/paidJobs/entities/employerPaidJobSubscription.entity';
export const userContractController = nestControllerContract(userContract);
export type UserRequestShapes = NestRequestShapes<
  typeof userContractController
>;

export type LoadedUser = Loaded<User, 'jobSeeker' | 'employer'>;

@Injectable()
export class UserService {
  /* eslint-disable max-params */
  constructor(
    @InjectRepository(User)
    private readonly userRepository: EntityRepository<User>,

    @InjectRepository(City)
    private readonly cityRepository: EntityRepository<City>,

    @InjectRepository(Language)
    private readonly languageRepository: EntityRepository<Language>,

    @InjectRepository(Subfunction)
    private readonly subfunctionRepository: EntityRepository<Subfunction>,

    @InjectRepository(Skill)
    private readonly skillRepository: EntityRepository<Skill>,

    private em: EntityManager,

    private readonly authService: AuthService,
  ) {}
  async validateUser(userId: number) {
    const user = await this.userRepository.findOne(userId, {
      populate: ['employer', 'jobSeeker'],
    });
    return user;
  }

  async getUser(id: number) {
    return this.userRepository.findOne({ id });
  }

  async createJobseeker(data: UserRequestShapes['createJobseeker']['body']) {
    const isUserExist = await this.userRepository.findOne({
      $or: [{ email: data.email }, { phone: data.phone }],
    });

    if (isUserExist?.email === data.email) {
      throw new BadRequestException('User email already exists');
    }
    if (isUserExist?.phone === data.phone) {
      throw new BadRequestException('User phone number already exists');
    }
    const [preferredLocations, languages, subfunctions, city, skill] =
      await Promise.all([
        this.cityRepository.find(data.preferredLocations),
        this.languageRepository.find(data.languages),
        this.subfunctionRepository.find(data.subfunction),
        this.cityRepository.findOneOrFail(data.city),
        this.skillRepository.find(data.skills),
      ]);

    if (data.preferredLocations.length !== preferredLocations.length) {
      throw new BadRequestException('Preferred locations not found');
    }
    if (data.languages.length !== languages.length) {
      throw new BadRequestException('Language not found');
    }

    if (data.subfunction.length !== subfunctions.length) {
      throw new BadRequestException('Subfunction not found');
    }

    const email = data.email.toLowerCase();
    const user = new User({
      firstName: data.firstName,
      lastName: data.lastName,
      email: email,
      role: UserRole.jobSeeker,
      phone: data.phone,
      picture: data.picture,
    });
    const jobSeeker = new JobSeeker({
      user: user,
      profileSummary: data.profileSummary,
      headline: data.headline,
      expectedSalaryInLpa: data.expectedSalaryInLpa,
      isPrivate: data.isPrivate,
      externalLink: data.externalLink,
      resume: data.resume,
      videoResume: data.videoResume,
      preferredLocations: preferredLocations,
      experienceInYear: data.experienceInYear,
      languages: languages,
      subfunction: subfunctions,
      city: city,
      skills: skill,
      noticePeriod: data.noticePeriod,
      workSchedule: data.workSchedule,
      socialMediaLink: data.socialMediaLink,
      isSubscribedToAlerts: data.isSubscribedToAlerts,
    });

    this.em.persist(jobSeeker);
    this.em.persist(user);

    await this.em.flush();

    const jwtToken = this.authService.getJwtToken(
      jobSeeker.user.id,
      jobSeeker.user.email,
    );

    return { jobSeeker, token: jwtToken };
  }

  async createEmployer(data: UserRequestShapes['createEmployer']['body']) {
    const isUserExist = await this.userRepository.findOne({
      $or: [{ email: data.email }, { phone: data.phone }],
    });
    if (isUserExist?.email === data.email) {
      throw new BadRequestException('User email already exists');
    }
    if (isUserExist?.phone === data.phone) {
      throw new BadRequestException('User phone number already exists');
    }
    const [city, companyIndustries] = await Promise.all([
      data.city ? this.em.findOne(City, { id: data.city }) : null,
      this.em.find(Industry, { id: { $in: data.company.industries } }),
    ]);

    const email = data.email.toLowerCase();
    const user = new User({
      firstName: data.firstName,
      lastName: data.lastName,
      email: email,
      phone: data.phone,
      role: UserRole.employer,
      picture: data.picture,
    });
    const company = new Company({
      name: data.company.name,
      website: data.company.website,
      industries: companyIndustries,
      aboutCompany: data.aboutCompany ?? '',
      companySize: data.companySize,
      logo: data.company.logo,
      gstNumber: data.gstNumber,
      gstAddress: data.gstAddress,
    });

    const employer = new Employer({
      user: user,
      company: company,
      verificationDocument: data.verificationDocument,
      city: city,
    });
    this.em.persist(employer);
    this.em.persist(user);

    await this.em.flush();
    const jwtToken = this.authService.getJwtToken(
      employer.user.id,
      employer.user.email,
    );

    return { employer, token: jwtToken };
  }
  async updateJobseeker(
    user: User,
    dto: UserRequestShapes['updateJobseeker']['body'],
  ) {
    const duplicatePhoneNumber = await this.userRepository.findOne({
      phone: dto.phone,
      id: { $ne: user.id },
    });

    if (duplicatePhoneNumber) {
      throw new BadRequestException('User phone number already exists');
    }
    wrap(user).assign({
      firstName: dto.firstName,
      lastName: dto.lastName,
      phone: dto.phone,
      picture: dto.picture,
    });
    const jobSeeker = user.jobSeeker;

    if (!jobSeeker) {
      throw new BadRequestException('JobSeeker not found');
    }

    wrap(jobSeeker).assign({
      profileSummary: dto.profileSummary,
      headline: dto.headline,
      expectedSalaryInLpa: dto.expectedSalaryInLpa,
      isPrivate: dto.isPrivate,
      externalLink: dto.externalLink,
      resume: dto.resume,
      videoResume: dto.videoResume,
      preferredLocations: dto.preferredLocations,
      experienceInYear: dto.experienceInYear,
      languages: dto.languages,
      subfunction: dto.subfunction,
      city: dto.city,
      skills: dto.skills,
      noticePeriod: dto.noticePeriod,
      workSchedule: dto.workSchedule,
      socialMediaLink: dto.socialMediaLink,
      isSubscribedToAlerts: dto.isSubscribedToAlerts,
    });

    await this.em.flush();
    return getSuccessMsg('Successfully updated');
  }

  async updateEmployer(
    dto: UserRequestShapes['updateEmployer']['body'],
    user: User,
  ) {
    const duplicatePhoneNumber = await this.userRepository.findOne({
      phone: dto.phone,
      id: { $ne: user.id },
    });

    if (duplicatePhoneNumber) {
      throw new BadRequestException('User phone number already exists');
    }
    const existingCompany = await this.em.findOne(Company, {
      name: { $ilike: dto.company.name },
    });

    const u = await this.em.populate(user, ['employer', 'employer.company']);
    const [companyIndustries] = await Promise.all([
      this.em.find(Industry, { id: { $in: dto.company.industries } }),
    ]);

    wrap(user).assign({
      firstName: dto.firstName,
      lastName: dto.lastName,
      phone: dto.phone,
      picture: dto.picture,
    });

    const employer = u.employer;

    if (!employer) {
      throw new BadRequestException('Employer not found');
    }
    if (existingCompany && existingCompany.id !== employer.company.id) {
      throw new BadRequestException('Company name already exists');
    }
    const company = employer.company;

    wrap(company).assign({
      name: dto.company.name,
      website: dto.company.website,
      industries: companyIndustries,
      aboutCompany: dto.aboutCompany ?? '',
      companySize: dto.companySize,
      logo: dto.company.logo,
      gstAddress: dto.gstAddress,
      gstNumber: dto.gstNumber,
    });
    wrap(employer).assign({
      verificationDocument: dto.verificationDocument,
      city: dto.city,
    });

    await this.em.flush();
    return getSuccessMsg('Successfully updated');
  }
  async getAllUsers() {
    const users = await this.userRepository.find({}, { limit: 10 });

    return {
      status: 200 as const,
      body: users.map((user) => ({
        firstName: user.firstName,
        lastName: user.lastName,
        email: user.email,
      })),
    };
  }

  async getJobseekerProfile(
    user: User,
  ): Promise<
    Loaded<
      User,
      | 'jobSeeker'
      | 'jobSeeker.preferredLocations'
      | 'jobSeeker.languages'
      | 'jobSeeker.subfunction'
      | 'jobSeeker.city'
      | 'jobSeeker.skills'
    >
  > {
    const u = await this.em.populate(user, [
      'jobSeeker',
      'jobSeeker.preferredLocations',
      'jobSeeker.languages',
      'jobSeeker.subfunction',
      'jobSeeker.city',
      'jobSeeker.skills',
    ]);

    if (!u.jobSeeker) {
      throw new BadRequestException('JobSeeker not found');
    }

    return u;
  }

  async getEmployerById(user: User) {
    const u = await this.em.populate(user, [
      'employer',
      'employer.city',
      'employer.company',
      'employer.company.industries',
    ]);

    const employer = u.employer;

    if (!employer) {
      throw new BadRequestException('Employer not found');
    }
    const company = employer.company;

    return {
      status: 200 as const,
      body: {
        employerId: user.id,
        firstName: user.firstName,
        lastName: user.lastName,
        email: user.email,
        phone: user.phone,
        role: UserRole.employer as const,
        company: {
          companyId: company.id,
          name: company.name,
          website: company.website,
          industries: company.industries.$.getItems().map((c: City) => ({
            id: c.id,
            name: c.name,
          })),
          logo: company.logo,
        },
        aboutCompany: employer.company.aboutCompany,
        companySize: employer.company.companySize,
        verificationDocument: employer.verificationDocument,
        city: employer.city
          ? { name: employer.city.name, id: employer.city.id }
          : null,
        picture: user.picture,
        isVerified: employer.isVerified,
        gstNumber: company.gstNumber,
        gstAddress: company.gstAddress,
      },
    };
  }
  async hasUserEverPurchasedSubscription({ employer }: { employer: Employer }) {
    const purchases = await this.em.find(
      EmployerJobSeekerSubscription,
      { employer },
      { fields: ['expiryDate'] },
    );

    const paidJobPurchases = await this.em.count(EmployerPaidJobSubscription, {
      employer,
      expiryDate: { $gte: new Date() },
    });

    return {
      hasHistoryOfPurchase: purchases.length > 0,
      isPurchaseActive: purchases.some((p) => p.expiryDate > new Date()),
      isJobBoostPurchaseActive: paidJobPurchases > 0,
    };
  }

  async viewJobseekerProfileFromManageApplication(
    dto: UserRequestShapes['viewJobseekerProfileFromManageApplication']['query'],
  ) {
    const { jobApplicationId } = dto;

    const jobApplication = await this.em.findOne(
      JobApplication,
      {
        id: jobApplicationId,
      },
      {
        populate: ['job'],
      },
    );

    if (!jobApplication) {
      throw new BadRequestException(
        'You cant see this jobseekers profile because he is not applying for this job',
      );
    }

    const jobSeeker = await this.em.findOne(
      JobSeeker,
      {
        user: { id: jobApplication.jobSeeker.user.id },
      },
      {
        populate: [
          'user',
          'skills',
          'languages',
          'subfunction',
          'preferredLocations',
          'city',
        ],
      },
    );
    if (!jobSeeker) {
      throw new BadRequestException('This jobSeeker is no longer available');
    }

    return jobSeeker;
  }
}
