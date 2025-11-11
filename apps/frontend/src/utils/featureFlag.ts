export const featureFlags = {
  unReleased: false,
};

export const getFreelancerTypeBasedOnEnv = (env: string) => {
  switch (env) {
    case "stage":
      return [5];
    case "prod":
      return [5]; // TODO : Confirm with db data
    case "dev":
    default:
      return [5];
  }
};

export const getFreelancerNicheBasedOnEnv = (env: string) => {
  switch (env) {
    case "stage":
      return [23];
    case "prod":
      return [23]; // TODO : Confirm with db data
    default:
    case "dev":
      return [23];
  }
};
