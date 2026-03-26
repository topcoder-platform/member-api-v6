/**
 * Contains all routes
 */
const {
  SCOPES: {
    MEMBERS
  },
  DELETE_USER_SCOPE
} = require('config')
const constants = require('../app-constants')
const { STATS_REFRESH, STATS_RERATE } = MEMBERS

module.exports = {
  '/members/health': {
    get: {
      controller: 'HealthController',
      method: 'checkHealth'
    }
  },
  '/members': {
    get: {
      controller: 'SearchController',
      method: 'searchMembers',
      auth: 'jwt',
      allowNoToken: true,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  },
  '/members/searchBySkills': {
    get: {
      controller: 'SearchController',
      method: 'searchMembersBySkills',
      auth: 'jwt',
      allowNoToken: true,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  },
  '/members/autocomplete': {
    get: {
      controller: 'SearchController',
      method: 'autocomplete',
      auth: 'jwt',
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  },
  '/members/autocomplete/:term': {
    get: {
      controller: 'SearchController',
      method: 'autocompleteByHandlePrefix',
      auth: 'jwt',
      scopes: [MEMBERS.READ, MEMBERS.ALL],
      access: ['copilot', 'administrator', 'admin']
    }
  },
  '/members/bulk-search': {
    post: {
      controller: 'SearchController',
      method: 'bulkSearch',
      auth: 'jwt',
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  },
  '/members/uid-signature': {
    get: {
      controller: 'MemberController',
      method: 'getMemberUserIdSignature',
      auth: 'jwt',
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  },
  '/members/:handle': {
    get: {
      controller: 'MemberController',
      method: 'getMember',
      auth: 'jwt',
      allowNoToken: true,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    },
    put: {
      controller: 'MemberController',
      method: 'updateMember',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    },
    delete: {
      controller: 'MemberController',
      method: 'deleteMember',
      auth: 'jwt',
      scopes: [DELETE_USER_SCOPE],
      access: constants.ADMIN_ROLES
    }
  },
  '/members/:handle/change_handle': {
    patch: {
      controller: 'MemberController',
      method: 'updateHandle',
      auth: 'jwt',
      access: constants.ADMIN_ROLES,
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/profileCompleteness': {
    get: {
      controller: 'MemberController',
      method: 'getProfileCompleteness',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/verify': {
    get: {
      controller: 'MemberController',
      method: 'verifyEmail',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/photo': {
    post: {
      controller: 'MemberController',
      method: 'uploadPhoto',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/confirmProfile': {
    post: {
      controller: 'MemberController',
      method: 'confirmProfileData',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/profileDownload': {
    get: {
      controller: 'MemberController',
      method: 'downloadProfile',
      auth: 'jwt',
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  },
  '/members/:handle/sendgrid-emails': {
    get: {
      controller: 'MemberController',
      method: 'getMemberSendgridEmails',
      auth: 'jwt',
      access: constants.ADMIN_ROLES,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  },
  '/members/:handle/traits': {
    get: {
      controller: 'MemberTraitController',
      method: 'getTraits',
      auth: 'jwt',
      allowNoToken: true,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    },
    post: {
      controller: 'MemberTraitController',
      method: 'createTraits',
      auth: 'jwt',
      scopes: [MEMBERS.CREATE, MEMBERS.ALL]
    },
    put: {
      controller: 'MemberTraitController',
      method: 'updateTraits',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    },
    delete: {
      controller: 'MemberTraitController',
      method: 'removeTraits',
      auth: 'jwt',
      scopes: [MEMBERS.DELETE, MEMBERS.ALL]
    }
  },
  '/members/stats/distribution': {
    get: {
      controller: 'StatisticsController',
      method: 'getDistribution'
    }
  },
  '/members/:handle/stats/history': {
    get: {
      controller: 'StatisticsController',
      method: 'getHistoryStats',
      auth: 'jwt',
      allowNoToken: true,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    },
    post: {
      controller: 'StatisticsController',
      method: 'createHistoryStats',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    },
    patch: {
      controller: 'StatisticsController',
      method: 'partiallyUpdateHistoryStats',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/stats/refresh': {
    post: {
      controller: 'StatisticsController',
      method: 'refreshMemberStats',
      auth: 'jwt',
      scopes: [STATS_REFRESH, MEMBERS.ALL],
      access: constants.ADMIN_ROLES
    }
  },
  '/members/:handle/stats/rerate': {
    post: {
      controller: 'StatisticsController',
      method: 'rerateMemberStats',
      auth: 'jwt',
      scopes: [STATS_RERATE, MEMBERS.ALL],
      access: constants.ADMIN_ROLES
    }
  },
  '/members/:handle/stats': {
    get: {
      controller: 'StatisticsController',
      method: 'getMemberStats',
      auth: 'jwt',
      allowNoToken: true,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    },
    post: {
      controller: 'StatisticsController',
      method: 'createMemberStats',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    },
    patch: {
      controller: 'StatisticsController',
      method: 'partiallyUpdateMemberStats',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/skills': {
    get: {
      controller: 'StatisticsController',
      method: 'getMemberSkills',
      auth: 'jwt',
      allowNoToken: true,
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    },
    post: {
      controller: 'StatisticsController',
      method: 'createMemberSkills',
      auth: 'jwt',
      scopes: [MEMBERS.CREATE, MEMBERS.ALL]
    },
    patch: {
      controller: 'StatisticsController',
      method: 'partiallyUpdateMemberSkills',
      auth: 'jwt',
      // access: constants.ADMIN_ROLES,
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/skills/verify': {
    post: {
      controller: 'StatisticsController',
      method: 'verifyMemberSkills',
      auth: 'jwt',
      scopes: [MEMBERS.UPDATE, MEMBERS.ALL]
    }
  },
  '/members/:handle/skills/:skillid': {
    get: {
      controller: 'MemberController',
      method: 'getMemberSkill',
      auth: 'jwt',
      scopes: [MEMBERS.READ, MEMBERS.ALL]
    }
  }
}
